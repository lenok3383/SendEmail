#!/usr/bin/env python
"""Simple utility program to build a set of configuration files.
Also it is able to build a file from its template.

The following file must exists:
    - _configvars.conf

Example content of this file:

    [rw_db_user]
    help=Read / Write Database User
    type=str
    default=writer
    validate_re='.*'

    [rw_db_password]
    help=Read / Write Database Password
    type=str
    validate_re='.*'

    [rw_db_timeout]
    help=Read / Write Database Timeout
    type=int
    default=10

Directory, that contains this file, must be specified in "CONFROOT"
environment variable or passed to this script via command args.

Also this directory may contain '_configfiles.conf' files.  Example content
of this file:

    db-template.conf, db.conf
    ldap-template.conf, ldap.conf

It contains pairs <template-file>, <output-file>.  Template file may
contain variables from _configvars.conf in form of "%s(variable_name)" or
"$variable_name", that will be replaced during processing.

:Status: $Id: //prod/main/_is/shared/python/conf/tools/configbuilder.py#15 $
:Authors: jwescott, bejung, rbodnarc
"""

import atexit
import optparse
import logging
import os
import re
import readline
import shutil
import string
import sys
import warnings

from ConfigParser import ConfigParser

from shared.conf import env
from shared.future.ordered_dict import OrderedDict

# Exceptions.

class ConfigBuilderException(Exception):
    """Base class for ConfigBuilder exceptions."""


class ConfigFileFormatError(ConfigBuilderException):
    """Error raised if one of configuration files has invalid format."""


class UnsupportedTypeError(ConfigBuilderException):
    """Error raised if variable from _configvars.conf has unknown type."""


class InvalidValueError(ConfigBuilderException):
    """Provided values are inconsistent with the defined variables."""


# Helper functions.

def parse_key_value_file(line_iterator, separator='=', comments=';#'):
    """Parses a file in "key-value" format.

    Files of this form contain keys and values separated by
    `separator`. Whitespace is ignored at the beginning or end of the line and
    around the separator. Keys cannot conatain the separator.

    :Parameters:
        - `line_iterator`: Something that yields lines when iterated over
        - `separator`: Separator between keys and values. (default '=')
        - `comments`: Lines starting with these characters are treated as
          comments (default ';#').
    """
    data = {}
    for line in line_iterator:
        line = line.strip()
        if not line or line[0] in comments:
            continue
        try:
            name, value = line.split(separator, 1)
        except ValueError:
            # Raise a more helpful error.
            raise ValueError('Could not parse line "%s"' % (line,))

        data[name.strip()] = value.strip()

    return data


# Configuration variables and _configvars.conf parsing.

class ConfigVariable(object):

    """Defines a configuration variable.

    This describes one stanza of a _configvars.conf file.  Notably,
    ConfigVariable objects only describe the variable, never the current config
    value.
    """

    __slots__ = 'name', 'help', 'default', 'validate_regex', 'var_type'

    DEFAULT_TYPE = 'str'

    CONFIG_TYPES = {'str' :   {'regex' : '.*',
                               'eval' : '%s'
                               },
                    'bool' :  {'regex' : '^(?:False|True)$',
                               'eval' : 'eval(%s)'
                               },
                    'int' :   {'regex' : r'^-?\d+$',
                               'eval' : 'eval(%s)'
                               },
                    'float' : {'regex' : r'^-?\d*\.?\d+$',
                                'eval' : 'eval(%s)'
                               },
                    'list' :  {'regex' : r'^\[.*\]$',
                               'eval' : '%s'
                               },
                    'dict' :  {'regex' : r'^\{.*\}$',
                               'eval' : '%s'
                               },
                    }


    def __init__(self, name, var_help=None, default=None, validate_regex=None,
                 var_type=None):
        self.name = name
        self.help = var_help
        self.default = default
        self.validate_regex = validate_regex

        if var_type is None:
            var_type = self.DEFAULT_TYPE
        if var_type not in self.CONFIG_TYPES:
            raise UnsupportedTypeError('Variable "%s" has unknown type "%s"' %
                                       (name, var_type))
        self.var_type = var_type

    def _effective_regex(self):
        """Returns the regex the value must match."""
        if self.validate_regex:
            return self.validate_regex
        else:
            return self.CONFIG_TYPES[self.var_type]['regex']

    def validate(self, value):
        """Checks that the provided `value` is valid for this variable."""
        if not re.match(self.CONFIG_TYPES[self.var_type]['regex'], value):
            return False
        if self.validate_regex and not re.match(self.validate_regex, value):
            return False
        return True

    def format(self, input_value):
        """Formats the provided `value` for substitution into config.

        This often involves wrapping the value in eval() for non string types.
        """
        if self.var_type != 'str' and input_value == 'None':
            return 'eval(None)'
        else:
            return self.CONFIG_TYPES[self.var_type]['eval'] % (input_value,)

    def __repr__(self):
        return '<ConfigVariable %s>' % (self.help,)

    def report(self, current_value=None):
        lines = []
        lines.append(self.help)
        lines.append('  VARIABLE: %s' % (self.name,))
        lines.append('  TYPE    : %s' % (self.var_type,))
        lines.append('  DEFAULT : %s' % (self.default,))
        if current_value:
            lines.append('  CURRENT : %s' % (current_value,))
        lines.append('  REGEX   : %s' % (self._effective_regex(),))
        lines.append('')
        return '\n'.join(lines)

    # Helper functions for comparing ConfigVariables -- mostly for unit tests.
    def _as_tuple(self):
        return tuple(getattr(self, slot) for slot in self.__slots__)

    def __cmp__(self, other):
        if isinstance(other, ConfigVariable):
            return cmp(self._as_tuple(), other._as_tuple())
        else:
            return NotImplemented


_KNOWN_OPTIONS = set(['prompt', 'order', 'help', 'default', 'type',
                      'validate_re'])

def parse_configvars_conf(configvars_file):
    """Parses the _configvars.conf file.

    :Returns: an ordered list of ConfigVariable objects.
    """
    order_warn_notice = True
    ordered_vars = list()

    with open(configvars_file) as fhandle:
        cp = ConfigParser(dict(), OrderedDict)
        cp.readfp(fhandle)

        for var in cp.sections():
            var_help = var
            default = None
            var_type = None
            validate_regex = None

            unknown_options = set(cp.options(var)) - _KNOWN_OPTIONS
            if unknown_options:
                warnings.warn('Unknown options %s for variable %s' %
                              (list(unknown_options), var))

            if cp.has_option(var, 'prompt'):
                var_help = cp.get(var, 'prompt')
            if cp.has_option(var, 'order') and order_warn_notice:
                warnings.warn('Option \'order\' is deprecated and it '
                              'is just ignored.',
                              DeprecationWarning)
                order_warn_notice = False
            if cp.has_option(var, 'help'):
                var_help = cp.get(var, 'help')
            if cp.has_option(var, 'default'):
                default = cp.get(var, 'default')
            if cp.has_option(var, 'type'):
                var_type = cp.get(var, 'type')
            if cp.has_option(var, 'validate_re'):
                validate_regex = cp.get(var, 'validate_re')

            ordered_vars.append(ConfigVariable(
                name=var, var_help=var_help, default=default,
                validate_regex=validate_regex, var_type=var_type))

    return ordered_vars


# Template substitution and formatting.

def format_vals(configvars, raw_values, use_defaults=False, ignore_extra=True):
    """Converts raw values (as entered by user) into formatted vals.

    This function uses the variable definitions to wrap values in eval() where
    appropriate. In addition, this function performs validation.

    :Parameters:
        - `configvars`: A list of ConfigurationVariable objects
        - `raw_values`: Values to be formatted as entered by user
        - `use_defaults`: (default False) if True, defaults will be used if
          values are missing from `raw_values`
        - `ignore_extra`: (default False) if True extra values will be ignored
          (otherwise InvalidValueError will be thrown)

    :Returns: an OrderedDict ready for substitution

    :Exceptions: `InvalidValueError` is raised when formatted values do not
        match the provided configuration variables.
    """
    raw_values = OrderedDict(raw_values)
    formatted_dict = OrderedDict()
    for variable in configvars:
        try:
            val = raw_values.pop(variable.name)
        except KeyError:
            val = None
        if val is None and use_defaults:
            val = variable.default
        if val is None:
            raise InvalidValueError('No value provided for %s' %
                                    (variable.name,))
        if not variable.validate(val):
            raise InvalidValueError('%s is an invalid value for variable %s' %
                                    (val, variable.name))

        formatted_dict[variable.name] = variable.format(val)

    if ignore_extra and raw_values:
        raise InvalidValueError('No matching variables found for keys %s' %
                                (raw_values.keys(),))

    return formatted_dict


def build_files(confroot, formatted_values):
    """Create auto-template configuration files and files from templates.

    First, this method tries to create configuration files from scratch for
    variables named like 'file.section.variable_name'.

    Next it tries to create files described in '_configfiles.conf'
    file (if exists) by replacing known patterns in their templates.

    Config files are assumed to be 'new-style' (using ${config_var}
    placeholders). If no substitutions are made, the template will be
    assumed to be an 'old-style' template (using %(config_var)s
    placeholders), and a deprecation warning will be thrown.
    """
    auto_configfiles = dict()
    for name, value in formatted_values.iteritems():
        if name.count('.') >= 2:
            fname, section, option = name.split('.', 2)
            cp = auto_configfiles.setdefault(fname,
                                             ConfigParser({}, OrderedDict))
            if not cp.has_section(section):
                cp.add_section(section)
            cp.set(section, option, value)

    for fname, cp in auto_configfiles.iteritems():
        fname = os.path.join(confroot, '%s.conf' % (fname,))
        with open(fname, 'wb') as fhandle:
            cp.write(fhandle)

            logging.info('File "%s" was created as auto-template '
                         'configuration file.' % (fname,))

    configfiles = _get_configfiles(confroot)

    old_style_warning = False

    for input_file, output_file in configfiles:
        input_path = os.path.join(confroot, input_file)
        output_path = os.path.join(confroot, output_file)
        temp_path = output_path + '.tmp'

        try:
            _build_new_style_file(input_path, temp_path, formatted_values)
        except ConfigFileFormatError:
            # Probably an old-style file.
            if not old_style_warning:
                warnings.warn(
                    'Template file had no substitutions. Assuming it was '
                    'a deprecated, old-style, template file. Please '
                    'convert to a new-style template '
                    '(e.g. ${config_var}). file=%s' % (input_path,),
                    DeprecationWarning)
                old_style_warning = True

            _build_old_style_file(input_path, temp_path, formatted_values)

        shutil.copymode(input_path, temp_path)
        os.rename(temp_path, output_path)

        logging.info('File "%s" was created from its template.'
                     % (output_path,))


def _get_configfiles(confroot):
    """Get pairs (<template-file>, <output-file>) from _configfiles file.

    :return: List of pairs (template-path, output-path).  If
            '_configfiles.conf' doesn't exist or is empty, empty list
            will be returned.
    """
    filename = os.path.join(confroot, '_configfiles.conf')

    if not os.path.exists(filename):
        return []

    with open(filename) as fhandle:
        try:
            configfiles = parse_key_value_file(fhandle, separator=',')
        except ValueError, e:
            raise ConfigFileFormatError(
                'Error parsing the file "%s". '
                'Expects <template-file>,<destination-file>. %s'
                % (filename, e))

    logging.info('Configuration files information read from "%s"'
                 % (filename,))

    return configfiles.items()


def _build_new_style_file(input_path, output_path, values):
    """Do "new style" substitution.  Unknown variables will be ignored.

    To substitute PRODROOT and CONFROOT by environment variables just
    use $prodroot and $confroot patterns.

    :param input_path: Path to template file.
    :param output_path: Path to output file.
    :param values_dict: Dictionary with variable names and values.
    """
    values_dict = values.copy()
    values_dict['confroot'] = env.get_conf_root()
    values_dict['prodroot'] = env.get_prod_root()

    with open(input_path) as fhandle:
        original = fhandle.read()
        template = string.Template(original)

    output = template.safe_substitute(values_dict)

    if original.replace('$$', '$') == output:
        # No substitutions found!
        raise ConfigFileFormatError('Not a new-style template file.')

    with open(output_path, 'w') as fhandle:
            fhandle.write(output)


def _build_old_style_file(input_path, output_path, values):
    """Do "old style" substitution.  Unknown variables will be ignored.

    :param input_path: Path to template file.
    :param output_path: Path to output file.
    :param values_dict: Dictionary with variable names and values.
    """
    values_dict = values.copy()
    values_dict['CONFROOT'] = env.get_conf_root()
    values_dict['PRODROOT'] = env.get_prod_root()

    with open(input_path) as fhandle:
        template = fhandle.read()

    # Escape any '%' symbols
    template = re.sub('%', '%%', template)

    # Unescape known variable
    known_vars = '|'.join(values_dict.keys())
    template = re.sub(r'%%%%\((%s)\)s' % (known_vars,),
                      r'%(\1)s', template)

    with open(output_path, 'w') as fhandle:
        fhandle.write(template % values_dict)


# Reporting functionality.

def report(configvars, values):
    """Get simple report about configuration variables.

    :return: String with report ready to print.
    """
    newvars = list()
    oldvalues = list()
    output = list()

    output.append('Current Options\n')
    output.append('===============\n')
    for variable in configvars:
        if not values.has_key(variable.name):
            newvars.append(variable)
        else:
            output.append(variable.report(
                values[variable.name]))
            oldvalues.append(variable.name)

    output.append('\nNew Options\n')
    output.append('===========\n')
    if newvars:
        for variable in newvars:
            output.append(variable.report())
    else:
        output.append('No new options.\n')

    output.append('\nDeprecated Options\n')
    output.append('==================\n')
    deprecated = [x for x in values.keys()
                  if not x in oldvalues]
    deprecated = [x for x in deprecated
                  if not x in ('PRODROOT', 'CONFROOT',
                               'prodroot', 'confroot')]
    if deprecated:
        for var in deprecated:
            output.append('%s=%s\n' % (var, values[var]))
    else:
        output.append('No deprecated options.\n')

    return ''.join(output)


# Prompting functionality.

def _raw_input(prompt):
    """Our own representation of built-in raw_input.

    Created for unit tests' purposes.
    """
    return raw_input(prompt)


def prompt_for_values(configvars, values, force=False):
    """Define type and value for each variable.

    If default value doesn't match validate regular expressions (both type
    regex and user defined regex from _configvars.conf file), display
    prompt for another value.

    User's input will be validated by mentioned regular expressions too.
    Also for variables of all types (except default 'str' type) user can
    input "None" and templates will be substituted with "eval(None)"
    string.

    To specify None or '' value for variable of default 'str' type user
    have to input "eval(None)" or "eval('')". (Empty strings can be
    selected if they are the default, but they cannot be entered at a
    prompt.)

    :param force: If True, prompt will be displayed anyway.
    :return: a list of pairs (var_name, var_value), where var_value
             is "raw", not wrapped with 'eval' variable value.
    """
    raw_values = list()

    logging.info('Gathering variable values...')

    enum_line_up = '%%%ds.' % (len(str(len(configvars))))

    for var_index, variable in enumerate(configvars):

        default_value = variable.default

        input_value = None

        if variable.name in values:
            default_value = values[variable.name]

        ask = False

        if (force or default_value is None or
            not variable.validate(default_value)):
            ask = True

        prompt = ''

        user_index = enum_line_up % (var_index + 1,)

        if default_value is not None:
            prompt = '%s %s (%s) [%s] = [%s]:' % (user_index,
                                                  variable.help,
                                                  variable.var_type,
                                                  variable.name,
                                                  default_value)
        else:
            prompt = '%s %s (%s) [%s]:' % (user_index,
                                           variable.help,
                                           variable.var_type,
                                           variable.name)

        if ask:
            while True:
                input_value = _raw_input(prompt).strip()

                if not input_value:
                    # If user just typed Enter, try to use default value if it
                    # exists, otherwise prompt again. (Don't accept empty value.)
                    if default_value is None:
                        continue
                    else:
                        input_value = default_value

                if (input_value == 'None' and variable.var_type != 'str') or \
                        variable.validate(input_value):
                    break
        else:
            input_value = default_value

        raw_values.append([variable.name, input_value])

    logging.info('Variable values gathered.')

    return raw_values


# Loading and storing configbuilder "cache" files.

def load_cache_values(filename):
    """Get the values saved during previous runs.

    :return: Dictionary with variable names as keys and
             variable types/values as values.
    """
    if not os.path.exists(filename):
        return dict()

    with open(filename) as fhandle:
        try:
            saved_values = parse_key_value_file(fhandle, separator='=')
        except ValueError, e:
            raise ConfigFileFormatError(
                'Error parsing the file "%s". Expects var_name=var_value. '
                '%s' % (filename, e))

    logging.info('Loaded saved variable values from "%s"' % (filename,))

    return saved_values


def save_cache_values(filename, values):
    """Store new variable values.

    :param values: List with [variable_name, variable_value] pairs.
    """
    with open(filename, 'wb') as fhandle:
        for name, value in values:
            fhandle.write('%s=%s\n' % (name, value))

    logging.info('Saved variable values to "%s"' % (filename,))


def get_cache_filename(savedir, confroot):
    """Get path to file where to save and to read saved variable values.

    :param savedir: Directory for a file.  Will be created if doesn't exist.
    :return: String with full path to a file.
    """
    if not os.access(savedir, os.R_OK | os.W_OK | os.X_OK):
        os.mkdir(savedir)
    filename = os.path.abspath(confroot)
    filename = filename.replace(os.path.sep, '_')
    filename = os.path.join(savedir, filename)
    return filename


# Main script functionality.

def do_configbuilder_report(confroot, savedir):
    """Report on values currently stored in cache file."""
    # Load saved values.
    cachefile = get_cache_filename(savedir, confroot)
    defaults = load_cache_values(cachefile)

    # Parse _configvars.conf
    configvars_file = os.path.join(confroot, '_configvars.conf')
    configvars = parse_configvars_conf(configvars_file)

    # Report on defined variables.
    return report(configvars, defaults)


def do_configbuilder_build_files(confroot, savedir, force=False):
    """Build configuration files.

    Reads currently stored values from the cache file, prompts for missing
    values and builds config files.
    """
    # Load saved values.
    cachefile = get_cache_filename(savedir, confroot)
    defaults = load_cache_values(cachefile)

    # Load _configvars.conf.
    configvars_file = os.path.join(confroot, '_configvars.conf')
    configvars = parse_configvars_conf(configvars_file)

    # Prompt for values
    save_values = prompt_for_values(configvars, defaults, force=force)

    # Build config files.
    substitution_values = format_vals(configvars, save_values)
    build_files(confroot, substitution_values)

    # Save provided values.
    save_cache_values(cachefile, save_values)


def setup_readline_history():
    """Loads configbuilder history file.

    Configbuilder will keep history of values entered if there is a
    ``.configbuilder_hist`` file in the user's home directory. While entering
    values, the user can hit <up> or <Ctrl-R> to access the history, just like
    in a shell.

    To avoid security concerns, this is available only if the user creates this
    file manually.
    """
    try:
        histfile = os.path.join(os.path.expanduser('~'), '.configbuilder_hist')
        if not os.path.exists(histfile):
            return
        try:
            readline.read_history_file(histfile)
        except IOError:
            pass
        atexit.register(readline.write_history_file, histfile)
    except Exception:
        pass


def main():
    """Script's entry point"""

    setup_readline_history()

    savedir_parent = os.environ.get('SAVEDIR')
    if savedir_parent is None:
        savedir_parent = os.environ.get('HOME')
    if savedir_parent is None:
        raise Exception('No $HOME directory defined.')

    default_savedir = os.path.join(savedir_parent, '.configbuilder')

    help_description = """Usage: %s [OPTIONS] [CONFROOT]

Program to build configuration files from templates.
If needed, prompt will be displayed for some configuration variables.

For example:
  Parameter 2 (int) [123]:

It means that you have to input value for "Parameter 1" which has type "int"
and default value "123".  If you just pressed "Enter" default value will be
used.  If you want to assign None to this variable, input "None".

Note!  To assign None or '' (empty string) value for variable of "str" type,
you have to input "eval(None)" or "eval('')".
"""

    # Parse command-line options.
    option_parser = optparse.OptionParser(usage=help_description %
                                          (os.path.basename(sys.argv[0])))

    option_parser.add_option('-v', '--verbosity', metavar='LVL',
                             action='store', type='int', dest='verbosity',
                             default=0,
                             help='Script output verbosity (0-3)')

    option_parser.add_option('-f', '--force', action='store_true',
                             dest='force',
                             help='Force re-prompting of configuration info.')
    option_parser.add_option('-d', '--describe',
                             action='store_true', dest='describe',
                             help='Print report about configuration values.')

    option_parser.add_option('-s', '--savedir', dest='savedir',
                             default=default_savedir,
                             help='Directory to save configbuilder files.')

    option_parser.add_option('-n', '--new_style', dest='new_style',
                             action='store_true',
                             help='Ignore. Previously, indicated that '
                             'templates are new-style, but this is assumed by '
                             'default.')

    (options, args) = option_parser.parse_args()

    if len(args) == 0:
        confroot = env.get_conf_root()
    elif len(args) == 1:
        confroot = args[0]
    else:
        option_parser.print_usage()
        return 1

    if options.verbosity <= 0:
        log_level = logging.ERROR
    elif options.verbosity == 1:
        log_level = logging.WARNING
    elif options.verbosity == 2:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level, format='%(message)s')

    if options.describe:
        print(do_configbuilder_report(confroot, options.savedir))
    else:
        try:
            do_configbuilder_build_files(confroot, options.savedir,
                                         force=options.force)
        except KeyboardInterrupt:
            print('Configuration interrupted.  Not all configuration files '
                  'have been written.  You should re-run the configbuilder '
                  'to ensure no corruption in configuration files.')
            return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
