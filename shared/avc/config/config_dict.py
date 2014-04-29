# Copyright (c) 2011-2013 Cisco Ironport Systems LLC.
# All rights reserved.
# Unauthorized redistribution prohibited.

"""AVC Library to parse Configuration files with configuration overrides

:Status: $Id: //prod/main/_is/shared/python/avc/config/config_dict.py#1 $
:Authors: cdherang
:Date: $DateTime: 2013/12/18 06:28:52 $
"""

__version__ = '$Revision: #1 $'


from shared.conf.config import Config


def config_list_to_dict(config, eval_globals=None):
    """Converts a list of configurations into a dictionary.

    :Parameters:
        `config`: list of configurations in the form
                    ['<section>.<option>=<value>',...]
        `eval_globals`: dictionary of namespace to use when using eval() to
                          load data
    :Returns:
        `ret_val`: dictionary of configurations in the form
                     {'<section>.<option>':'<value>',...}
    :Exceptions:
        `ValueError`: raises on missing configuration value.
    """
    ret_val = dict()
    if config is not None:
        for item in config:
            if '=' not in item:
                raise ValueError('Missing configuration value. '
                                 'Use format <section>.<option>=<value>')

            key, val = item.split('=', 1)
            key, val = key.strip(), val.strip()
            do_eval = False
            if val.startswith('eval(') and val.endswith(')'):
                do_eval = True
                val = val[5:-1]
            elif ((val.startswith('[') and val.endswith(']')) or
                    (val.startswith('{') and val.endswith('}'))):
                do_eval = True

            if do_eval:
                val = eval(val, eval_globals)

            ret_val[key] = val

    return ret_val


class ConfigDictWithOverrides(Config):

    """Extension of ConfigDict with support for overrides dictionary."""

    def __init__(self, config_path, override_data=None):
        """Initialises the instance of ConfigDictWithOverrides class.

        :Parameters:
            `config_path`: path to the config file.
            `override_data`: dictionary of override configuration.
        """
        Config.__init__(self, config_path)
        if override_data is not None:
            self._override = override_data
        else:
            self._override = dict()

    def set_override(self, key, value):
        """Sets the override configuration value.

        This can also be used to add new override configurations.

        :Parameters:
            `key`: config option name.
            `value`: config option value.
        """
        self._override[key] = value

    def __getitem__(self, item):
        """Gets the overridden config item by name.

        :Parameters:
            `item`: config option name (keyword).
        :Return:
            The overridden item value if exists or the original value otherwise.
        """
        try:
            return self._override[item]
        except KeyError:
            return Config.__getitem__(self, item)

    def get(self, key, default=None):
        """Gets the overridden the value stored at the provided key

        :Parameters:
            `key`: Key to use as a lookup.
            `default`: Optional Parameter. Returned in case the key does not exist.
        :Returns:
            The overridden value stored at the provided key, or default if it
            does not exist.
        """
        try:
            return self[key]
        except KeyError:
            return default

    def get_or_exc(self, name):
        """Get the config item from the dictionary.

        :Parameters:
            `name`: the name of the item to get.
        :Return:
            config item value if exists or the KeyError exception otherwise.
        """
        return self[name]

    def iteritems(self):
        """Iterates over all the configurations.

        :Return:
            configuration dictionary iterator object.
        """
        full_config = dict(Config.get_snapshot(self).iteritems())
        full_config.update(self._override)

        return full_config.iteritems()
