"""Common configuration module.

Maintains a cache of configurations which are loaded from config files or seed
data.  Reloads when cache becomes stale.

Example Usage:

1. Read latest value

This mode attempts to reload the configuration at each lookup request.
If the file has changed it will load the latest values from the file.
It is possible that two consecutive gets could return values from different
versions of a file.

conf = shared.conf.get_config('demo_config')
my_val = conf.get('cat0.key0', 3600)
another_val = conf['cat0.key0']

2. Thread-safe read-only snapshot

This module provides two ways to get a snapshot of the configuration.

It is safe to read multiple values from the snapshot as they are
necessarily from the same file.

Using with syntax:

with shared.conf.get_config('demo_config') as c:
  my_val = c.get('cat0.key0', 3600)
  another_val = c['cat0.key0']

Obtaining a snapshot object:

conf = shared.conf.get_config('demo_config')
ro_conf = conf.get_snapshot()

3. Unit testing

In a unit test, you may wish to set up a config object with key-value pairs to
use in place of the configuration file. (Generally in unit testing, we do not
have configuration files, so it's usually easier to provide key-value pairs
than to write a file to disk.)

Unit test setup:

class MyTest(unittest.TestCase):

   def setUp(self):
       new_conf = shared.conf.config.ConfigFromDict(
           {'section1.foo': 'bar', 'section2.controls': 'heart of the sun'})

       self.addCleanup(shared.conf.config.clear_config_cache)
       shared.conf.config.set_config('my_app', new_conf)

Note that the above uses self.addCleanup() to ensure that
shared.conf.config.clear_config_cache() is called after the test is run.

With this setUp, code that calls shared.conf.get_config('my_app') will get the
config object set up above, and it will not try to look for ``my_app.conf``.

:Status: $Id: //prod/main/_is/shared/python/conf/config.py#8 $
:Authors: lromanov
"""

import collections
import ConfigParser as configparser
import os
import threading

import shared.conf.env

_config_cache = {}


class ConfigFileMissingError(Exception):
    """Error raised when a configuration file is missing."""
    def __init__(self, filename):
        Exception.__init__(self,
                           'Configuration file missing: %s' % (filename,))


class ConfigurationParseError(Exception):
    """Error raised when a configuration option can't be parsed properly."""
    def __init__(self, option, error):
        Exception.__init__(self, 'Error parsing configuration value %s: %s' %
                           (option, error))


class ConfigSnapshot(collections.Mapping):

    """Read-only snapshot of configuration."""

    __slots__ = ('_elements', '_eval_cache', '_sections')

    def __init__(self, data, sections):
        self._elements = data
        self._eval_cache = {}
        self._sections = sections

    def __getitem__(self, key):
        do_eval = False
        value = self._elements[key]

        if not isinstance(value, basestring):
            # Not a string, no point evaluating it.
            return value

        if key in self._eval_cache:
            return self._eval_cache[key]

        if value.startswith('eval(') and value.endswith(')'):
            do_eval = True
            value = value[5:-1]
        elif ((value.startswith('[') and value.endswith(']')) or
              (value.startswith('{') and value.endswith('}'))):
            do_eval = True

        if do_eval:
            try:
                self._eval_cache[key] = eval(value)
            except Exception, e:
                raise ConfigurationParseError(value, e)
            return self._eval_cache[key]
        else:
            return self._elements[key]

    def __iter__(self):
        return iter(self._elements)

    def __len__(self):
        return len(self._elements)

    def __str__(self):
        return str(self._elements)

    def get(self, key, default=None, evaluate=True):
        """Retrieve the value stored at the provided key.

        :Parameters:
        - key: Key to use as a lookup.
        - default: Returned in case the key does not exist.
        - evaluate: By default eval the string if it begins with `eval` or
        resembles a list or dict. Can be set to false to return the raw string.

        :Returns:
        The value stored at the provided key, or default if it does not exist.
        """
        if not evaluate:
            return self._elements.get(key, default)
        else:
            return collections.Mapping.get(self, key, default)

    def sections(self):
        """Return the config sections as a list.

        :Parameters:
        - None

        :Returns:
        - The sections of the stored config as a list.
        """
        return self._sections


class Config(object):

    """Maintains an update-to-date dict-like configuration object.

    This class provides access to a configuration file, and reloads itself
    every time the underlying file is modified.

    To ensure consistency between two consecutives gets, obtain a config
    snapshot with get_snapshot or make use of the with syntax.

    Note: This should usually not be instantiated directly. If you are trying
    to get product configuration, you should use shared.conf.get_config() to
    get it.
    """

    def __init__(self, path):
        self.path = path
        self._file_mtime = 0
        self._conf = None
        self._conf_lock = threading.RLock()

    def __getitem__(self, key):
        self._reload_config()
        return self._conf[key]

    def get(self, key, default=None):
        """Retrieves the value stored at the provided key.

        This may reload the configuration file from disk if it has changed.

        :Parameters:
        - key: Used as the lookup to the dictionary.
        - default: Returned if the provided key does not exist in the dict.

        :Returns:
        Value stored at the provided key or default if the key does not exist.
        """
        self._reload_config()
        return self._conf.get(key, default)

    def get_snapshot(self):
        """Refresh the configuration and return a copy of it.

        :Parameters:
        - None

        :Returns:
        - A copy of the configuration dictionary.
        """
        self._reload_config()
        return self._conf

    def _reload_config(self):
        with self._conf_lock:
            try:
                current_file_mtime = os.path.getmtime(self.path)
            except OSError:
                raise ConfigFileMissingError(self.path)
            if self._file_mtime != current_file_mtime:
                self._conf = _load_config(self.path)
                self._file_mtime = current_file_mtime

    def __str__(self):
        self._reload_config()
        return str(self._conf)

    def __enter__(self):
        return self.get_snapshot()

    def __exit__(self, type, value, traceback):
        pass


class ConfigFromDict(Config):

    """A Config() object filled with static data.

    This is meant to be used to replace a standard Config() object in unit
    tests with one that provides static data.

    Standard configuration objects have string values which will be evaluated
    if they meet certain formats. ConfigFromDict objects can have non-string
    values, but these will be returned even if get(key, evaluate=False) is
    requested.
    """

    def __init__(self, config_dict=None):
        Config.__init__(self, None)

        my_copy = config_dict.copy()

        # Emulate sections by parsing keys as "section.key".
        sections = set((k.split('.')[0] for k in my_copy))

        self._conf = ConfigSnapshot(my_copy, list(sections))

    def _reload_config(self):
        pass


def get_config(name):
    """Returns a Config object for a specified name.

    The requested file should exist in the CONFROOT and have an extension of
    `conf`. For example ``get_config('db')`` will read the db.conf file from
    CONFROOT.

    This behavior can be overidden with set_config(), for example in unit
    tests.

    :Parameters:
    - filename: The base name of the file (without extension) to load the
      config from.

    :Returns:
    - A Config object for the specified file.
    """
    assert not name.endswith('.conf'), \
        'get_config() takes a short name, e.g. "db", only'
    assert not name.startswith('/'), \
        'get_config() takes files relative to confroot, e.g. "db" or "dbs/db1"'
    try:
        return _config_cache[name]
    except KeyError, e:
        full_path = os.path.join(shared.conf.env.get_conf_root(),
                                 '%s.conf' % (name,))
        return _config_cache.setdefault(name, Config(full_path))


def set_config(filename, data):
    """Set the cached configuration to the provided data.

    This would generally be used in unit tests to override configuration
    file data. This method associates the provided `data` with the `filename`.
    Subsequent calls to `get_config` with this `filename` will return `data`.

    :Parameters:
    - filename: The base name of the file to load the config data into.
    - data: The data to associate with the provided `filename`.

    :Returns:
    - None
    """
    _config_cache[filename] = data

def clear_config_cache():
    """Clears the config cache.

    Used to undo any previous calls of `set_config`.
    """
    _config_cache.clear()


def _load_config(filename):
    """Parse a config file into a dictionary and return it.

    :Parameters:
    - filename: absolute path to a config file to parse.
    - raw: ConfigParser.SafeConfigParser raw parameter.

    :Returns:
    - Dictionary with keys in a 'section.options' format.
    """

    config = {}
    cp = configparser.SafeConfigParser()
    cp.read(filename)
    for sec in cp.sections():
        name = sec.lower()
        for opt in cp.options(sec):
            val = cp.get(sec, opt, raw=True).strip()
            config['%s.%s' % (name, opt.lower())] = val
    return ConfigSnapshot(config, cp.sections())
