"""Provides named counters.

The idea of this module is that it will keep counters and keep
historical snapshots at certain intervals. These histories can be used
to display charts. In addition, the module can use defined storage classes
to store the values and rates (ft counts are an example).

There are three types of counters. The first type is StaticCounter. It is
designed to keep track of how many times an event occurred.  This type can
also be used to set timestamps. The second type is a DynamicCounter counter
that keeps track of both current value and rates. This can be very useful
for tracking application load for instance.

The easiest way to use this module is through the convinience functions
provided:

1. shared.monitor.counter.init(StorageClass, **kwargs), where kwargs are the
   initialization arguments for StorageClass (db config, for example).

2. history is upadated automatically when any counter write operation
   occurs (set/increment/decrement).

3. call shared.monitor.counter.flush_counters() occationally to write the
   counter values to storage.

Alternately, clients can call ``counter.start(interval, max_history,
[use_db=True])`` to start a thread whose purpose is to initialize and
update counter history. (For counter.start, *interval* and *max_history*
can be integers or functions that return integers. ``counter.stop()``
can be used to stop the thread.)

To change/get counter values, the following methods are provided:

``counter.increase(counter_name, step=1)``
  Increases the counter by the specified value.

``counter.decrease(counter_name, step=1)``
  Decreases the counter by the specified value.

``counter.set_value(counter_name, value)``
  Sets the counter to the specified value.

``counter.get_value(counter_name)``
  Gets the value of the specified counter.

Counters are displayed on the status page hierarchically, and a ``:``
is used as the separator.

FT Programs which subclass FTDaemon will include counter support automatically
and will use the common.ft_server.util.FTCounterStorage for counter storage.

Non-FT programs may use this library as well, providing their own
CounterStorage class for persistent storage (if necessary).

:Status: $Id $
:Authors: bjung
"""


import collections
import json
import threading
import time

import shared.viz.chart_helper


now = time.time

_counter_css = \
"""<style type="text/css"><!--
.counter_list {
    padding-left: 0px;
}
.counter_item {
    height: 18px;
    list-style-type: none;
}
.counter_item:hover {
    background-color: #dedede;
}
.counter_values {
    float: right;
    margin-right: 0px;
}
.counter_value {
    text-align: left;
    display: inline-block;
    width: 200px;
}
.counter_rate {
    text-align: left;
    display: inline-block;
    width: 200px;
}
.counter_timestamp {
    display: inline-block;
    text-align: left;
    width: 200px;
}
--></style>"""


DEFAULT_COUNTER_UPDATE_INTERVAL = 60
COUNTER_HISTORY_DEPTH = 60


_G_COUNTERS_LOCK = threading.RLock()
_G_COUNTERS = dict()
_G_COUNTERS_KEY_NAME_MAP = dict()


def with_lock(func):
    """RLock decorator."""

    with _G_COUNTERS_LOCK:
        return func

# Classes to support counters.  There should be no need to instantiate any
# of the following classes explicitly.  Use the convenience functions.


class InvalidCounterError(Exception):
    pass


class ExistingCounterError(Exception):
    pass


class BaseCounterStorage(object):

    """Base Counter Storage class"""

    def __init__(self, **kwargs):
        """Do any initialization here, for instance, setting up the database"""
        pass

    def store(self, key, value, name):
        """Actually store the value"""
        pass


class NullCounterStorage(BaseCounterStorage):
    """Counter 'Storage' object that drops all values."""
    pass


class ThreadSafeCounter(object):

    """Thread-safe Counter"""

    def __init__(self):
        self._lock = threading.RLock()
        self._value = {}
        self._touched = True

    def increase(self, step=1):
        """Increase the value of the counter by 'step'."""

        key = threading.current_thread().name
        thr_value = self._value.get(key, 0)
        self._value[key] = thr_value + step
        self._touched = True

    def decrease(self, step=1):
        """Decrease the value of the counter by 'step'."""

        key = threading.current_thread().name
        thr_value = self._value.get(key, 0)
        self._value[key] = thr_value - step
        self._touched = True

    def reset(self):
        """Reset the value of the counter to zero."""

        self._value.clear()
        self._touched = True

    def get_value(self):
        """Return the value of the counter."""

        with self._lock:
            return sum(self._value.values())

    def set_value(self, val):
        """Set the value of the counter."""

        with self._lock:
            self._value.clear()
            self._value[threading.current_thread().name] = val
        self._touched = True

    def get_and_set_touched(self):
        """Return True if the value has been updated, reset touched to False.

        * note: there is a race condition with getting touched.  It is possible
        that a counter will return untouched when it has indeed been touched.
        It was decided that it is better for touched to be less accurate than
        to introduce more locks."""

        try:
            return self._touched
        finally:
            self._touched = False

    def __str__(self):
        return str(self.get_value())


class BaseCounter(object):

    """Class which encapsulates base counter functions."""

    delimiter = ':'
    _storage_instance = None

    @classmethod
    def set_storage(cls, storage_instance):
        """Set the storage class for the BaseCounter (and all subclasses)."""

        assert isinstance(storage_instance, BaseCounterStorage)
        cls._storage_instance = storage_instance

    def __init__(self, name, reset_value=0, min_interval=None,
                 max_history_depth=None, store_on_update_only=True, **kwargs):
        """Constructor for the base counter.

        :param name: specific name of the counter (counters can be grouped
            together based on sections in the name, sections are separated by
            the delimiter)
        :param reset_value: value the counter is reset to, defaults to 0
        :param min_interval: interval which history is updated
        :param max_history_depth: set a limit for the amount of history data
            points we keep
        """

        self._reset_value = reset_value
        self.name = name
        self.key = self._define_key()
        self._history = collections.deque()
        if max_history_depth is None:
            max_history_depth = COUNTER_HISTORY_DEPTH
        self._max_history_depth = max_history_depth
        self._counter = ThreadSafeCounter()
        if min_interval is None:
            min_interval = DEFAULT_COUNTER_UPDATE_INTERVAL
        self.set_min_interval(min_interval)
        self.store_on_update_only = store_on_update_only
        self._start_time = now()
        self._interval_start_time = now()

    def _define_key(self):
        return self.name.lower()

    @property
    def value(self):
        return self._counter.get_value()

    def touched(self):
        """Returns whether this counter has been updated, resets touch flag."""
        return self._counter.get_and_set_touched()

    def set_min_interval(self, interval):
        assert interval >= 0, 'interval value must be zero or greater'
        self._min_interval = interval

    def reset(self):
        """Reset the counter to the reset_value"""

        self._counter.set_value(self._reset_value)

    def _update_history(self):
        dt = now() - self._interval_start_time
        if self._min_interval and dt > self._min_interval:
            self._history.append((now(), self._counter.get_value()))
            self._interval_start_time = now()
        while len(self._history) > self._max_history_depth:
            self._history.popleft()

    def set(self, value):
        """Set the counter to the value passed in

        :param value: an integer value
        """

        self._update_history()
        self._counter.set_value(value)

    def flush(self):
        """Store the value using the storage_class"""

        self._update_history()
        if not self.store_on_update_only or (
            self.store_on_update_only and self.touched()):
            self._storage_instance.store(self.key, self.value, self.name)

    def html(self):
        """HTML representation of this counter."""

        return \
"""
<div class="counter_row">
    <span class="counter_key">%s</span>
    <span class="counter_values">
        <span class="counter_value">
            <a href="/counter_detail?name=%s&type=value">
            current value: %s
            </a>
        </span>
    </span>
</div>
""" % (self.key, self.name, self.value)

    def chart(self):
        """Return the HTML for displaying the counter history on a chart."""

        values = [(time.strftime('%H:%M:%S', time.localtime(ts)), v) \
                    for (ts, v) in self._history]
        if not values:
            return 'no value data yet...'
        return shared.viz.chart_helper.gviz_simple_chart(
            values, xlabels_column=0, series_labels=['time', 'value'],
            chart_type=shared.viz.chart_helper.LINE_CHART, title=self.key)

    def json(self):
        """Return a JSON representation of the counter data"""

        data = {}
        data['type'] = self.__class__.__name__
        data['name'] = self.name
        data['key'] = self.key
        data['value'] = self.value
        data['history'] = list(self._history)
        return json.dumps(data)


class StaticCounter(BaseCounter):

    """Class for handling static counters.  Examples of static counters
    may be timestamps, or logging failures.  Basically any type of counter
    where rate does not apply."""

    suffix = 'st'

    def __init__(self, name, reset_value=0, min_interval=None,
                 max_history_depth=None, timestamp=False, **kwargs):
        """Constructor for the static counter.

        :param name: specific name of the counter
        :param reset_value: value the counter is reset to, defaults to 0
        :param min_interval: interval which history is updated
        :param max_history_depth: set a limit for the amount of history data
            points we keep
        """
        super(StaticCounter, self).__init__(
            name, reset_value, min_interval=min_interval,
            max_history_depth=max_history_depth)

        self.set(self._reset_value)

    def _define_key(self):
        return self.name.lower() + self.delimiter + self.suffix


class Timestamp(BaseCounter):

    """Class for timestamp "counters"."""

    suffix = 'ts'

    def __init__(self, name, reset_value=0, min_interval=None,
                 max_history_depth=None, **kwargs):
        """Constructor for the static counter.

        :param name: specific name of the counter
        :param reset_value: value the counter is reset to, defaults to 0
        :param min_interval: interval which history is updated
        :param max_history_depth: set a limit for the amount of history data
            points we keep
        """
        super(Timestamp, self).__init__(
            name, reset_value, min_interval=min_interval,
            max_history_depth=max_history_depth)

        self.set(self._reset_value)

    def _define_key(self):
        return self.name.lower() + self.delimiter + self.suffix

    def html(self):
        """HTML representation of this counter."""

        return \
"""
<div class="counter_row">
    <span class="counter_key">%s</span>
    <span class="counter_values">
    <span class="counter_timestamp">ts: %s</span>
    </span>
</div>
""" % (self.key,
       time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.value)))

    def chart(self):
        """Return the HTML for displaying the counter history on a chart."""
        return ''


class DynamicCounter(BaseCounter):

    """Class for handling dynamic counters.  Dynamic counters have rates
    associated with them.
    """
    suffix = 'dyn'
    rate_suffix = 'r%s' # The rate calculation interval is substituted in.

    def __init__(self, name, reset_value=0, min_interval=None,
                 max_history_depth=None, **kwargs):
        """Constructor for the dynamic counter.

        :param section: used for grouping counters together, could be
            a component name
        :param name: specific name of the counter
        :param reset_value: value the counter is reset to, defaults to 0
        :param min_interval: interval which history is updated
        :param max_history_depth: set a limit for the amount of history data
            points we keep
        """
        super(DynamicCounter, self).__init__(
            name, reset_value, min_interval=min_interval,
            max_history_depth=max_history_depth)
        self.rate_key = self._define_rate_key()
        self._rate = 0
        self._rate_history = collections.deque()
        self.set(self._reset_value)

    def _define_key(self):
        return self.name.lower() + self.delimiter + self.suffix

    def _define_rate_key(self):
        suffix = self.rate_suffix % (int(self._min_interval),)
        return self.name.lower() + self.delimiter + suffix

    def set_min_interval(self, interval):
        super(DynamicCounter, self).set_min_interval(interval)
        self.rate_key = self._define_rate_key()

    def increment(self, step=1):
        """Increment the counter by 'step'.

        :param step: the amount to increment the counter
        """

        self._update_history()
        self._counter.increase(step)

        return self.value

    def decrement(self, step=1):
        """Decrement the counter by 'step'.

        :param step: the amount to decrement the counter
        """
        return self.increment(-step)

    def _update_history(self):
        """Update the internal history dictionary with rate information for
        the counter.
        """
        # The last value is used to determine the change in value.
        last_value = 0
        last_update = 0
        if self._history:
            last_update, last_value = self._history[-1]

        ts = now()
        dt = ts - self._interval_start_time

        # The average does not depend on the interval.
        try:
            average = self.value / (ts - self._start_time)
        except ZeroDivisionError:
            # This can happen if _update_history is called right on startup.
            average = 0.0

        # If it's time to update the rate info (based on _min_interval) do it.
        if self._min_interval and dt > self._min_interval:
            self._history.append((ts, self.value))
            current = (self.value - last_value) / dt
            self._rate_history.append((ts, (current, average)))
            self._interval_start_time = ts

        # If min_interval is set to 0, history not really kept.
        if not self._min_interval:
            # Set the current rate to the average.
            self._rate_history = collections.deque([(ts, (average, average))])

        # Make sure history doesn't grow beyond its bounds.
        while len(self._history) > self._max_history_depth:
            self._history.popleft()
        while len(self._rate_history) > self._max_history_depth:
            self._rate_history.popleft()

    @property
    def rate(self):
        # Only return if we have some history, otherwise results are
        # inconsistent.
        if self._rate_history:
            return self._rate_history[-1][1]
        else:
            return (0.0, 0.0)

    def flush(self):
        """Store the value using the storage_class"""

        # Explicitely call update since they won't have been updated if set has
        # not been called.
        self._update_history()
        self._storage_instance.store(self.key, self.value, self.name)
        self._storage_instance.store(self.rate_key, self.rate[0], self.name)

    def html(self):
        """HTML representation of this counter."""

        return \
"""
<div class="counter_row">
    <span class="counter_key">%s</span>
    <span class="counter_values">
        <span class="counter_value">
            <a href="/counter_detail?name=%s&type=value">
            current value: %s
            </a>
        </span>
        <span class="counter_rate">
            <a href="/counter_detail?name=%s&type=rate">
            current rate: %.3f
            </a>
        </span>
        <span class="counter_rate">average rate: %.3f</span>
    </span>
</div>
""" % (self.key, self.name, self.value, self.name,
       self.rate[0], self.rate[1])

    def chart(self, rate=False):
        """Return the HTML for displaying the counter history on a chart.

        :param rate: True if the chart represents a rate
        """
        if rate:
            rates = [(time.strftime('%H:%M:%S', time.localtime(ts)), r[0])
                     for (ts, r) in self._rate_history]
            if not rates:
                return 'no rate data yet...'
            return shared.viz.chart_helper.gviz_simple_chart(
                rates, xlabels_column=0, series_labels=['time', 'rate'],
                chart_type=shared.viz.chart_helper.LINE_CHART, title=self.rate_key)
        return super(DynamicCounter, self).chart()

    def json(self):
        """Return a JSON representation of the counter data."""

        data = {}
        data['type'] = self.__class__.__name__
        data['name'] = self.name
        data['key'] = self.key
        data['rate_key'] = self.rate_key
        data['value'] = self.value
        data['rate'] = self.rate
        data['history'] = list(self._history)
        data['rate_history'] = \
            [(ts, r[0], r[1]) for (ts, r) in self._rate_history]
        return json.dumps(data)


# Counter convenience functions.


@with_lock
def init(storage_instance):
    """Initialize the counters.  This should be called once at the start
    of the process which is including counters.

    :param storage_instance: an instance of a subclass of BaseStorageClass
    """
    BaseCounter.set_storage(storage_instance)


def _init_counter(counter_class, name, min_interval=None,
                  max_history_depth=None, pass_on_existing=True, **kwargs):
    """Initialize the given counter class.

    :param counter_class: the class of the counter to be initialized
    """
    if pass_on_existing and name in _G_COUNTERS:
        if not isinstance(_G_COUNTERS[name], counter_class):
            raise ExistingCounterError('The counter %s already exists and '
                'is of a different class' % (name,))
        return
    if name in _G_COUNTERS:
        raise ExistingCounterError(
            'The counter %s already exists' % (name,))
    return counter_class(name, min_interval=min_interval,
                         max_history_depth=max_history_depth, **kwargs)


@with_lock
def init_static_counter(name, min_interval=None, max_history_depth=None,
                        **kwargs):
    """Initialize a static counter.

    :param name: specific name of the counter
    :param kwargs: keyword arguments specific to the StaticCounter class
    """
    counter = _init_counter(StaticCounter, name, min_interval=min_interval,
                            max_history_depth=max_history_depth, **kwargs)
    if counter:
        _G_COUNTERS[name] = counter
        _G_COUNTERS_KEY_NAME_MAP[counter.key] = name


@with_lock
def init_timestamp_counter(name, min_interval=None, max_history_depth=None,
                           **kwargs):
    """Initialize a timestamp (static) counter.

    :param name: specific name of the counter
    :param kwargs: keyword arguments specific to the StaticCounter class
    """
    counter = _init_counter(Timestamp, name, min_interval=min_interval,
                            max_history_depth=max_history_depth, **kwargs)
    if counter:
        _G_COUNTERS[name] = counter
        _G_COUNTERS_KEY_NAME_MAP[counter.key] = name


@with_lock
def init_dynamic_counter(name, min_interval=None, max_history_depth=None,
                         **kwargs):
    """Initialize a dynamic counter.

    :param name: specific name of the counter
    :param kwargs: keyword arguments specific to the DynamicCounter class
    """
    counter = _init_counter(DynamicCounter, name, min_interval=min_interval,
                            max_history_depth=max_history_depth, **kwargs)
    if counter:
        _G_COUNTERS[name] = counter
        _G_COUNTERS_KEY_NAME_MAP[counter.key] = name
        _G_COUNTERS_KEY_NAME_MAP[counter.rate_key] = name


def increase(name, step=1, **kwargs):
    """Increase a dynamic counter.  If the counter has not been initialized
    a new dynamic counter will be created with default values.

    :param name: specific name of the counter
    :param step: the amount to increment the counter
    """
    # No thread safety concerns, since we check in init.
    if not name in _G_COUNTERS:
        init_dynamic_counter(name, pass_on_existing=True, **kwargs)
    _G_COUNTERS[name].increment(step)


def decrease(name, step=1, **kwargs):
    """Decrease a dynamic counter.  If the counter has not been initialized
    a new dynamic counter will be created with default values.

    :param name: specific name of the counter
    :param step: the amount to decrement the counter
    """
    # No thread safety concerns, since we check in init.
    if not name in _G_COUNTERS:
        init_dynamic_counter(name, pass_on_existing=True, **kwargs)
    _G_COUNTERS[name].decrement(step)


def set_value(name, value, **kwargs):
    """Set the value of a counter.  If the counter has not been initialized
    a new static counter will be created with default values.

    :param name: specific name of the counter
    :param value: an integer value
    """
    # No thread safety concerns, since we check in init.
    if not name in _G_COUNTERS:
        init_static_counter(name, pass_on_existing=True, **kwargs)
    _G_COUNTERS[name].set(value)


def timestamp(name, timestamp=None, **kwargs):
    """Set the value of a timestamp (static) counter.  If the counter has not
    been initialized a new timestamp counter will be created with
    default values.

    :param name: specific name of the counter
    :param timestamp: an integer timestamp, if None is give, the current time
        will be used
    :raises: InvalidCounterError
    """
    # No thread safety concerns, since we check in init.
    if not name in _G_COUNTERS:
        init_timestamp_counter(name, pass_on_existing=True, **kwargs)
    if timestamp is None:
        timestamp = now()
    if not isinstance(_G_COUNTERS[name], Timestamp):
        raise InvalidCounterError(
            'The counter %s is not a timestamp.' % (name,))
    _G_COUNTERS[name].set(timestamp)


@with_lock
def exists(name):
    """Does the counter exist

    :param name: specific name of a counter
    """
    return name in _G_COUNTERS


@with_lock
def reset(name, ignore_errors=False):
    """Reset the value of a counter.

    :param name: specific name of the counter
    :raises: InvalidCounterError
    """
    counter = _G_COUNTERS.get(name, None)
    if counter:
        counter.reset()
    elif not ignore_errors:
        raise InvalidCounterError(
            'The counter %s does not exist.' % (name,))


def reset_counters_for_section(section):
    """Reset the value of all counters for a given section.

    :param section: used for grouping counters together, could be
        a component name
    """
    counters = get_counters_for_section(section)
    for counter in counters:
        counter.reset()


def reset_all_counters():
    for counter in get_all_counters():
        counter.reset()


def get_value(name):
    """Get the value of a counter.

    :param name: specific name of the counter
    :returns: an integer value
    :raises: InvalidCounterError
    """
    return get_counter(name).value


def get_rate(name):
    """Get the rate of a counter.

    :param name: specific name of the counter
    :returns: an float value
    :raises: InvalidCounterError
    """
    return get_counter(name).rate


def get_chart(name):
    """Get the rate of a counter.

    :param name: specific name of the counter
    :returns: an float value
    :raises: InvalidCounterError
    """
    return get_counter(name).chart()


@with_lock
def get_sections():
    """Get the names of all the valid counter sections.

    :returns: a list of section names
    """
    sections = []
    # Don't use an iterator since the counters dict may change size.
    for name in _G_COUNTERS.keys():
        section = name.split(BaseCounter.delimiter, 1)[0]
        if section in sections:
            continue
        sections.append(section)
    sections.sort()
    return sections


@with_lock
def get_counters_for_section(section):
    """Get the all the counters for a given section.

    :param section: used for grouping counters together, could be
        a component name
    :returns: a list of counters
    """
    counter_names = []
    sectioned_keys = \
        [k.split(BaseCounter.delimiter) for k in _G_COUNTERS.keys()]
    # A Section may be passed in a string or tuple.
    if isinstance(section, str):
        section = section.split(BaseCounter.delimiter)
    # Don't use an iterator since the counters dict may change size.
    for name in _G_COUNTERS.keys():
        parts = name.split(BaseCounter.delimiter)
        skip = False
        for i, s in enumerate(section):
            if parts[i] != s:
                skip = True
                break
        if not skip:
            counter_names.append(name)

    return [_G_COUNTERS[n] for n in counter_names]


@with_lock
def get_all_counters():
    """Get the all the counters.

    :returns: a list of counters
    """
    return _G_COUNTERS.values()


@with_lock
def get_counter_from_key(key):
    name = _G_COUNTERS_KEY_NAME_MAP.get(key, None)
    if name:
        return _G_COUNTERS[name]
    else:
        raise InvalidCounterError('No counter associated with key %s' % (key,))


@with_lock
def get_counter(name, raise_error=False):
    """Return a counter object.

    :param name: name of counter
    """
    counter = _G_COUNTERS.get(name, None)
    if counter or not raise_error:
        return counter
    else:
        raise InvalidCounterError('The counter %s does not exist.' % (name,))


def flush_counters():
    """Flush the counters.  Saving the values with the defined storage_class.
    """
    for counter in get_all_counters():
        counter.flush()


# Web interface related functions, these are generally used within FT, but
# could be used with minihttp anywhere.


def web_counters(server, path, params, fout):
    """For use with minihttp/FT.  Create an HTML page of counters."""

    sections = get_sections()
    html_sections = []
    for section in sections:
        s_counters = get_counters_for_section(section)
        html = '<h3 class="section_header">%s</h3>' % (section,)
        html += '<ul class="counter_list">\n'
        for counter in s_counters:
            html += '<li class="counter_item">%s</li>\n' % (counter.html(),)
        html += '</ul>\n'
        html_sections.append(html)
    html = '<hr class="divider" />'.join(html_sections)
    html += _counter_css
    fout.write(html)


def web_counter_detail(server, path, params, fout):
    """For use with minihttp/FT.  Create an HTML page of a specific counter."""

    name = params['name'][0]
    counter_type = params['type'][0]
    counter = get_counter(name)
    fout.write(shared.viz.chart_helper.import_js_libs())
    if counter_type == 'rate':
        fout.write(counter.chart(rate=True))
    else:
        fout.write(counter.chart())


# Counter updating thread functions.


_counter_thread = None
_counter_thread_stop_event = None

def set_counter_history_depth(history_depth):
    """Sets the default history depth for counters initialized after this call.
    :history_depth: int, how much history to keep for all counters.
    """
    global COUNTER_HISTORY_DEPTH
    COUNTER_HISTORY_DEPTH = history_depth

def start(update_interval, **kwargs):
    """Start a thread to call _update_history periodically.  start() must
    be called with parameters indicating how often to update the
    history and how much history to keep. These should be integers or
    functions that return integers. the ``use_db`` keyword argument is
    used to determine whether the FT database should be updated with
    the counter values. (Default: True)
    """

    global _counter_thread
    global _counter_thread_stop_event

    _counter_thread_stop_event = threading.Event()
    _counter_thread = threading.Thread(target=_loop, name='counter',
                                       args=[update_interval], kwargs=kwargs)
    _counter_thread.setDaemon(True)
    _counter_thread.start()


def _loop(interval, use_db=True):
    while True:
        _counter_thread_stop_event.wait(_int_or_callable(interval))
        if (not _counter_thread_stop_event or
            _counter_thread_stop_event.isSet()):
            return
        if use_db:
            flush_counters()


def _int_or_callable(val):
    if callable(val):
        return int(val())
    elif isinstance(val, int):
        return val
    else:
        raise ValueError('Invalid argument')


def stop(timeout=None):
    global _counter_thread
    global _counter_thread_stop_event

    _counter_thread_stop_event.set()
    _counter_thread.join(timeout)

    _counter_thread_stop_event = None
    _counter_thread = None
