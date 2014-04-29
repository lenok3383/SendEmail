#!/usr/bin/env python
"""IPConf - generate an easily readable and editable file that can be used to
generate a configbuilder cache file.

:Status: $Id: //prod/main/_is/shared/python/conf/tools/ipconf_gen.py#2 $
:Authors: bwhitela
"""
import optparse
import os
import pwd
import re
import socket
import sys
import time

import shared.conf.env
from shared.conf.tools import configbuilder
from shared.conf.tools import ipconf
from shared.file import md5


def get_file_id(filename):
    """Try to get the Id macro from the input filename.

    :Parameters:
        `filename`: The path to a readable file with which to extract the Id
                    macro contents.

    :Returns: A string with the contents of the Id macro, <hostname>:<filename>
              otherwise.
    """
    id_regx = re.compile('^.*\$Id\:(.*)\$')
    with open(filename, 'r') as fh:
        for line in fh:
            match = id_regx.match(line)
            if match:
                return match.group(1).strip()
    hostname = socket.gethostname()
    return '%s:%s' % (hostname, filename)


def gen_header(file_id, md5):
    """Generate the header string for intermediate config files.

    Output should be in the form:

    #User <username> created this file from <configvars id macro> on <date>
    cbldr_config_var_version=<hex of configvars md5>

    :Parameters:
        - `file_id`: The 'Id' macro of the _configvars file.
        - `md5`: A string of hex that represents the md5 of the _configvars
                 file.

    :Returns: A string to be used as the header of the easily editable
              intermediate configuration files.
    """
    user = pwd.getpwuid(os.getuid())[0]
    header = '# User %s created this file from %s on %s\n' \
             '%s=%s\n\n' % (user, file_id, time.ctime(),
             ipconf.CONF_VERSION_CHECKSUM_VAR, md5)
    return header


def gen_file_content(configvars, cached_vals=None):
    """Generate content for easily editable intermediate configuration files.

    Format for the file looks like this:

    #This is the help statement.
    conf_var_name=1234

    :Parameters:
        - `configvars`: A list of configbuilder.ConfigVariable objects.
        - `cached_vals`: A dict of previously cached configvar variables.

    :Returns: A string to be written to a file for easy editing.
    """
    out_sections = []
    error_count = 0
    for configvar in configvars:
        name = configvar.name

        if configvar.help:
            help = configvar.help
        else:
            help = ''

        if cached_vals and name in cached_vals:
            # Try to upgrade.
            val = cached_vals[name]
            var = '%s=%s' % (name, val)
            if val == '':
                out_sections.append('# NOTE: No default/cached value '
                                    'specified!\n# %s\n%s' % (help, var))
            elif configvar.validate(val):
                out_sections.append('# %s\n%s' % (help, var))
            else:
                error_count +=1
                out_sections.append('# ERROR: Failed validation! type: %s, '
                                    'regex: %s\n# %s\n%s' %
                                    (configvar.var_type,
                                     configvar.validate_regex, help, var))
        else:
            if configvar.default:
                var = ';%s=%s' % (name, configvar.default)
                out_sections.append('# %s\n%s' % (help, var))
            else:
                var = '%s=' % (name,)
                out_sections.append('# NOTE: No default/cached value '
                                    'specified!\n# %s\n%s' % (help, var))


    if error_count:
        if error_count == 1:
            return '#\n# %d ERROR found when upgrading cache file!\n#\n\n' % \
                   (error_count) + '\n\n'.join(out_sections)
        else:
            return '#\n# %d ERRORS found when upgrading cache file!\n#\n\n' % \
                   (error_count) + '\n\n'.join(out_sections)
    else:
        return '\n\n'.join(out_sections)


def main():
    """Script's entry point"""

    option_parser = optparse.OptionParser(
        usage='%prog [options] <output_path>',
        epilog='<output_path> is the output path (with file name) where the '
               'editable intermediate configuration file should be saved. '
               'If this file already exists it will be upgraded.')

    option_parser.set_defaults(confroot=shared.conf.env.get_conf_root())

    option_parser.add_option(
        '-v', '--verbose', action='store_true', dest='verbose',
        help='Be more verbose.')
    option_parser.add_option(
        '-c', '--confroot', action='store', dest='confroot',
        help='Path to product configuration directory (default %default).')

    (options, args) = option_parser.parse_args()

    ipconf.configure_logging(options)

    if len(args) != 1:
        logging.error('Please specify the output path.')
        return 1

    outfile = args[0]

    # Check and get values from file to be upgraded.
    if os.path.exists(outfile):
        cached_vals = configbuilder.load_cache_values(outfile)
    else:
        cached_vals = None

    # Load _configvars.conf and get relevant info.
    configvars_file = os.path.join(options.confroot, '_configvars.conf')
    configvars_md5 = md5.compute_file_md5(configvars_file).hexdigest()
    configvars_id = get_file_id(configvars_file)
    configvars = configbuilder.parse_configvars_conf(configvars_file)

    # Write out easily editable intermediate file.
    with open(outfile, 'w') as output:
        output.write(gen_header(configvars_id, configvars_md5))
        output.write(gen_file_content(configvars, cached_vals))

    return 0


if __name__ == '__main__':
    sys.exit(main())
