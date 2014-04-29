"""Common time functions.

During the porting to Python 2.6 we've made the following changes
which break compatibility with existing IronPort products:

    datetimeToOffset - you must use timeutils.datetime_to_timestamp instead.
    parseDateStr - you must use timeutils.datestring_to_datetime instead.
    get_epoch_time - you must use timeutils.datestring_to_timestamp instead.
    convert_datestr_to_long - you must use timeutils.datestring_to_timestamp
                              instead.
    convert_long_to_datestr - you must use timeutils.timestamp_to_datestring
                              instead.
    onemonthago - you must use timeutils.lastmonth instead.
    onemonthago_gmt - you must use timeutils.lastmonth_gmt instead.

:Status: $Id: //prod/main/_is/shared/python/util/timeutils.py#1

:Authors: bbrahms, gperry, ohmelevs
"""

import calendar
import datetime
import time

from _strptime import LocaleTime, TimeRE


ONEMINUTE = 60
ONEHOUR = ONEMINUTE * 60
ONEDAY = ONEHOUR * 24
ONEWEEK = ONEDAY * 7
ONEMONTH = ONEDAY * 30  # not really, but we do this all over the place
ONEYEAR = ONEDAY * 365

DATE_FORMAT = '%Y-%m-%d %H:%M:%S %Z'


TIME_ZONES = {
    'IDLW': '-12', 'NT': '-11', 'HST': '-10', 'CAT': '-10', 'AHST': '-10',
    'AKST': '-09', 'YST': '-09', 'HDT': '-09', 'AKDT': '-08', 'YDT': '-08',
    'PST': '-08', 'PDT': '-07', 'MST': '-07', 'MDT': '-06', 'CST': '-06',
    'CDT': '-05', 'EST': '-05', 'SAT': '-04', 'EDT': '-04', 'AST': '-04',
    '#NST': '-03.5', 'NFT': '-03.5', '#GST': '-03', '#BST': '-03',
    'ADT': '-03', 'NDT': '-02.5', 'AT': '-02', 'SAST': '-02', 'WAT': '-01',
    'GMT': '+00', 'UT': '+00', 'UTC': '+00', 'WET': '+00', 'WEST': '+00',
    'CET': '+01', 'FWT': '+01', 'MET': '+01', 'MEZ': '+01', 'MEWT': '+01',
    'SWT': '+01', 'BST': '+01', 'GB': '+01', 'CEST': '+02', 'EET': '+02',
    'FST': '+02', 'MEST': '+02', 'MESZ': '+02', 'METDST': '+02', 'SST': '+02',
    'EEST': '+03', 'BT': '+03', 'MSK': '+03', 'IT': '+03.5', 'ZP4': '+04',
    'MSD': '+03', 'ZP5': '+05', 'IST': '+05.5', 'ZP6': '+06', 'NST': '+06.5',
    '#SST': '+07', 'CCT': '+08', 'AWST': '+08', 'WST': '+08', 'PHT': '+08',
    'JST': '+09', 'ROK': '+09', 'CAST': '+09.5', 'EAST': '+10', 'GST': '+10',
    'CADT': '+10.5', 'EADT': '+11', 'IDLE': '+12', 'NZST': '+12', 'NZT': '+12',
    'NZDT': '+13'}


class TZ(datetime.tzinfo):
    """Utility class to handle datetime time zones."""

    def __init__(self, tz_name):
        self.tz_name = tz_name
        self.offset = TIME_ZONES.get(tz_name)
        if not self.offset:
            raise ValueError('No such time zone.')
        datetime.tzinfo.__init__(self)

    def tzname(self, dt):
        """Return time zone name.

        :Parameters:
            - `dt`: datetime object.

        :Return:
            Time zone name.
        """
        return self.tz_name

    def dst(self, dt):
        """Return daylight saving.

        :Parameters:
            - `dt`: datetime object.

        :Return:
            Daylight saving.
        """
        return datetime.timedelta(0)

    def utcoffset(self, dt):
        """Return UTC offset.

        :Parameters:
            - `dt`: datetime object.

        :Return:
            UTC offset.
        """
        values = self.offset.split('.')
        m = 0
        if len(values) == 2:
            m = int(values[1])
        return datetime.timedelta(hours=int(values[0]), minutes=m)


def first_second_of_day_gmt(time_tuple, offset=0):
    """Calculates Unix timestamp for the first second of the day.

    :Parameters:
        - `time_tuple`: local time tuple.

        - `offset`: The number of days to add. Positive values refer to
                  days in the future, negative ones, to days in the past.

    :Return:
        Unix timestamp (GMT time) for the first second of the specified day.
    """
    gmtime = list(time_tuple)
    gmtime[8] = -1
    gmtime[3:8] = [0] * 5
    gmtime[2] += offset

    return calendar.timegm(gmtime)


def first_second_of_day(time_tuple, offset=0):
    """Calculates Unix timestamp for the first second of the day.

    :Parameters:
        - `time_tuple`: local time tuple.

        - `offset`: The number of days to add. Positive values refer to
                  days in the future, negative ones, to days in the past.

    :Return:
        Unix timestamp (local time) for the first second of the specified day.
    """
    localtime = list(time_tuple)
    localtime[8] = -1
    localtime[3:8] = [0] * 5
    localtime[2] += offset

    return time.mktime(localtime)


def first_second_of_month_gmt(time_tuple, offset=0):
    """Calculates Unix timestamp for the first second of the month.

    :Parameters:
        - `time_tuple`: local time tuple.

        - `offset`: The number of months to add. Positive values refer to
                  months in the future, negative ones, to months in the past.

    :Return:
        Unix timestamp (GMT time) for the first second of the specified month.
    """
    time_list = list(time_tuple)
    time_list[2] = 1
    time_list[1] += offset
    gmtime = list(time.localtime(time.mktime(time_list)))
    gmtime[8] = -1
    gmtime[3:7] = [0] * 4
    return calendar.timegm(gmtime)



def first_second_of_month(time_tuple, offset=0):
    """Calculates Unix timestamp for the first second of the month.

    :Parameters:
        - `time_tuple`: local time tuple.

        - `offset`: The number of days to add. Positive values refer to
                  days in the future, negative ones, to days in the past.

    :Return:
        Unix timestamp (local time) for the first second of the
        specified month.
    """
    localtime = list(time_tuple)
    localtime[8] = -1
    localtime[3:8] = [0] * 5
    localtime[2] = 1
    localtime[1] += offset

    return time.mktime(localtime)


def today(unixtime=None):
    """Return the Unix timestamp for the first second of today or of the
    specific day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (local time) for the first second of the specified
        day or today.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_day(time.localtime(unixtime))


def today_gmt(unixtime=None):
    """Return the Unix timestamp for the first second of today or of the
    specific day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (GMT time) for the first second of the specified
        day or today.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_day_gmt(time.gmtime(unixtime))


def yesterday(unixtime=None):
    """Return the Unix timestamp for the first second of a previous day
    or of the day before specified timestamp.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (local time) for the first second of the day before
        specified day or yesterday.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_day(time.localtime(unixtime), -1)


def yesterday_gmt(unixtime=None):
    """Return the Unix timestamp for the first second of a previous day
    or of the day before specified timestamp.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (GMT time) for the first second of the day before
        specified day or yesterday.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_day_gmt(time.gmtime(unixtime), -1)


def weekago(unixtime=None):
    """Return the Unix timestamp for the first second of a day week ago.

    If unix timestamp is specified function will return the first second
    of a day a week before specified day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (local time) for the first second of the day week ago.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_day(time.localtime(unixtime), -7)


def weekago_gmt(unixtime=None):
    """Return the Unix timestamp for the first second of a day week ago.

    If unix timestamp is specified function will return the first second
    of a day a week before specified day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (GMT time) for the first second of the day week ago.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_day_gmt(time.gmtime(unixtime), -7)


def lastmonth(unixtime=None):
    """Return the Unix timestamp for the first second of a day month ago.

    If unix timestamp is specified function will return the first second
    of a day a month before specified day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (local time) for the first second of the day month ago.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_month(time.localtime(unixtime), -1)


def lastmonth_gmt(unixtime=None):
    """Return the Unix timestamp for the first second of a day month ago.

    If unix timestamp is specified function will return the first second
    of a day a month before specified day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (GMT time) for the first second of the day month ago.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_month_gmt(time.gmtime(unixtime), -1)


def twomonthsago(unixtime=None):
    """Return the Unix timestamp for the first second of a day two months ago.

    If unix timestamp is specified function will return the first second of a
    day two months before specified day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (local time) for the first second of the day two
        months ago.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_month(time.localtime(unixtime), -2)


def twomonthsago_gmt(unixtime=None):
    """Return the Unix timestamp for the first second of a day two months ago.

    If unix timestamp is specified function will return the first second of a
    day two months before specified day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (GMT time) for the first second of the day two
        months ago.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_month_gmt(time.gmtime(unixtime), -2)


def thismonth(unixtime=None):
    """Return the Unix timestamp for the first second of this month.

    If unix timestamp is specified function will return the first second
    of the month of the specified day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (local time) for the first second of the month.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_month(time.localtime(unixtime))


def thismonth_gmt(unixtime=None):
    """Return the Unix timestamp for the first second of this month.

    If unix timestamp is specified function will return the first second
    of the month of the specified day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (GMT time) for the first second of the month.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_month_gmt(time.gmtime(unixtime))


def nextmonth(unixtime=None):
    """Return the Unix timestamp for the first second of the next month.

    If unix timestamp is specified function will return the first second
    of the next month of the specified day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (local time) for the first second of the next month.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_month(time.localtime(unixtime), +1)


def nextmonth_gmt(unixtime=None):
    """Return the Unix timestamp for the first second of the next month.

    If unix timestamp is specified function will return the first second
    of the next month of the specified day.

    :Parameters:
        - `unixtime`: specific timestamp.

    :Return:
        Unix timestamp (GMT time) for the first second of the next month.
    """
    if unixtime is None:
        unixtime = time.time()
    return first_second_of_month_gmt(time.gmtime(unixtime), +1)


def add_seconds(t, delta):
    """Modify given time by delta seconds.

    :Parameters:
        - `t`: specific timestamp.
        - `delta`: time delta (int).

    :Return:
        Modified unix timestamp.
    """
    return t + delta


def add_minutes(t, delta):
    """Modify given time by delta minutes.

    :Parameters:
        - `t`: specific timestamp.
        - `delta`: time delta (int).

    :Return:
        Modified unix timestamp.
    """
    return t + ONEMINUTE * delta


def add_hours(t, delta):
    """Modify given time by delta hours.

    :Parameters:
        - `t`: specific timestamp.
        - `delta`: time delta (int).

    :Return:
        Modified unix timestamp.
    """
    return t + ONEHOUR * delta


def add_days(t, delta):
    """Modify given time by delta days.

    :Parameters:
        - `t`: specific timestamp.
        - `delta`: time delta (int).

    :Return:
        Modified unix timestamp.
    """
    return t + ONEDAY * delta


def add_weeks(t, delta):
    """Modify given time by delta weeks.

    :Parameters:
        - `t`: specific timestamp.
        - `delta`: time delta (int).

    :Return:
        Modified unix timestamp.
    """
    return add_days(t, delta * 7)


def add_months(t, delta, start_t=None):
    """Modify given time by delta months.

    :Parameters:
        - `t`: specific timestamp.
        - `delta`: time delta (int).
        - `start_t`: timestamp to obtain start day of the period to add.

    :Return:
        Modified unix timestamp.
    """

    y, M, d, h, m, s, wd, yd, ds = time.localtime(t)

    if delta % 12 == 0:
        y += delta // 12
    else:
        M += delta
        if M > 12 or M < 1:
            y += M // 12
            M = M % 12

    if start_t:
        d = time.localtime(start_t)[2]
    month_range = calendar.monthrange(y, M)
    if d > month_range[1]:
        d = month_range[1]

    return time.mktime([y, M, d, h, m, s, wd, yd, ds])


def add_years(t, delta, start_t=None):
    """Modify given time by delta years.

    :Parameters:
        - `t`: specific timestamp.
        - `delta`: time delta (int).
        - `start_t`: timestamp to obtain start day of the period to add.

    :Return:
        Modified unix timestamp.
    """
    return add_months(t, delta * 12, start_t)


__UNIT_FUNC = {'s': add_seconds,
               'm': add_minutes,
               'h': add_hours,
               'd': add_days,
               'w': add_weeks,
               'M': add_months,
               'y': add_years}


def add_time(t, delta, units, start_t=None):
    """Modify given time by delta specified units.

    :Parameters:
        - `t`: specific timestamp.
        - `delta`: time delta (int).
        - `units`: time units. Can be:
                 'w' - weeks, 'h' - hours, 'm' - minutes, 's' - seconds,
                 'y' - years, 'M' - months, 'd' - days
        - `start_t`: timestamp to obtain start day of the period to add.
                   Makes sense only for months or years because of the
                   different amounts of days in a month.

    :Return:
        Modified unix timestamp.

    :Exceptions:
        - `ValueError`: if wrong time unit was specified.
    """

    func = __UNIT_FUNC.get(units)
    if not func:
        raise ValueError('No such time unit: %s.' % (str(units),))
    if units in ('M', 'y'):
        return func(t, delta, start_t)
    else:
        return func(t, delta)


def get_time_intervals(start_time, end_time, interval, units):
    """Splits a time period into a smaller periods configured by input
    parameters.

    :Parameters:
        - `start_time`: start timestamp of the period.
        - `end_time`: end timestamp of the period.
                    if end time greater than start time, periods will be
                    calculated in backward order.
        - `interval`: specifies the interval to split te entire period with.
                    For backward calculation parameter must be negative.
        - `units`: specifies interval units. Value can be:
                     hour or h, day or d, month or m, year or Y.

    :Return:
        List of the lists in format:
            [[start_timestamp1, end_timestamp1],
             [start_timestamp2, end_timestamp2]]

    :Exceptions:
        - `ValueError`: if wrong unit type was specified.
    """
    if units == 'h' or units == 'hour':
        deltaunit = 'h'
    elif units == 'd' or units == 'day':
        deltaunit = 'd'
    elif units == 'm' or units == 'month':
        deltaunit = 'M'
    elif units == 'y' or units == 'year':
        deltaunit = 'y'
    else:
        raise ValueError('Wrong unit type.')

    result = []
    current = start_time
    if start_time < end_time and interval > 0:
        while long(current) < long(end_time):
            end = add_time(current, long(interval), deltaunit, start_time)
            if long(end) > long(end_time):
                end = end_time
            result.append([current, end])
            current = end
    if start_time > end_time and interval < 0:
        while long(current) > long(end_time):
            end = add_time(current, long(interval), deltaunit, start_time)
            if long(end) < long(end_time):
                end = end_time
            result.append([end, current])
            current = end
    return result


def timestamp_to_datestring(timestamp, format=DATE_FORMAT, zone='UTC'):
    """Converts unix timestamp into date sting.

    :Parameters:
        - `timestamp`: specific timestamp.
        - `format`: output format. DATE_FORMAT - default.
        - `zone`: output time zone. Specified name must be listed in
                  TIME_ZONES. UTC - default.

    :Return:
        Date string in specified format.

    :Exceptions:
        - `ValueError`: if specified time zone isn't in TIME_ZONES dictionary.
    """
    dt = datetime.datetime.fromtimestamp(timestamp, TZ(zone))
    return dt.strftime(format)


def datestring_to_datetime(datestr, format=DATE_FORMAT, default_tz=None):
    """Converts date sting to datetime object.

    :Parameters:
        - `datestr`: specific timestamp.
        - `format`: output format. DATE_FORMAT - default.
        - `default_tz`: use this time zone for convertion to timestamp. If this
                      parameter isn't specified %Z from format is used. If
                      both variants are not specified UTC is used. If both
                      variants are specified default_tz parameter will be used.

    :Return:
        Datetime object.

    :Exceptions:
        - `ValueError`: if wrong date string format was specified.
        - `ValueError`: if wrong time zone was specified.
    """
    if datestr is None:
        return None

    locale_time = LocaleTime()
    locale_time.timezone += (TIME_ZONES.keys(),)
    found = TimeRE(locale_time=locale_time).compile(format).match(datestr)
    if not found:
        raise ValueError('Incorrect date string format.')
    date_dict = found.groupdict()

    if not default_tz:
        default_tz = date_dict.get('Z', 'UTC')
    return datetime.datetime(int(date_dict.get('Y', 1900)),
                             int(date_dict.get('m', 1)),
                             int(date_dict.get('d', 1)),
                             int(date_dict.get('H', 0)),
                             int(date_dict.get('M', 0)),
                             int(date_dict.get('S', 0)),
                             tzinfo=TZ(default_tz))


def datetime_to_timestamp(dt):
    """Converts datetime object to unix timestamp.

    :Parameters:
        - `dt`: datetime object.

    :Return:
        Unix timestamp.
    """
    return calendar.timegm(dt.utctimetuple())


def datestring_to_timestamp(datestr, format=DATE_FORMAT, default_tz=None):
    """Converts date sting to unix timestamp.

    This function is a wrapper to the datestring_to_datetime function.

    :Parameters:
        - `datestr`: specific timestamp.
        - `format`: output format. DATE_FORMAT - default.
        - `default_tz`: use this time zone for convertion to timestamp. If this
                      parameter isn't specified %Z from format is used. If
                      both variants are not specified UTC is used.

    :Return:
        Unix timestamp.

    :Exceptions:
        - `ValueError`: if wrong date string format was specified.
        - `ValueError`: if wrong time zone was specified.
        - `AssertionError`: if time zone is specified in the format string and
                          also as a parameter.
    """
    dt = datestring_to_datetime(datestr, format, default_tz)
    return datetime_to_timestamp(dt)

