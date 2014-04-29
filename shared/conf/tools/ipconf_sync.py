#!/usr/bin/env python
"""IPConf - sync config from Perforce

:Author: duncan
:Status: $Id: //prod/main/_is/shared/python/conf/tools/ipconf_sync.py#6 $
"""
import logging
import os
import sys

import shared.conf.env
import shared.conf.tools.configbuilder
import shared.file.md5
import shared.scm.perforce
from shared.conf.tools import configbuilder
from shared.conf.tools import ipconf


def get_perforce_data(path):
    """Reads data from Perforce for the given Perforce path."""
    with shared.scm.perforce.Perforce() as p:
        return p.get(path)


def _get_perforce_paths(target, options):
    """Picks paths to search based on options."""
    paths = ipconf.get_perforce_paths(target, options)
    if options.revision:
        revision = options.revision
        if revision.isdigit():
            revision = '@' + revision
        paths = [path + revision for path in paths]
    return paths


def build_config_files(confroot, cache_data, force=False):
    """Builds configuration files from the provided cache file."""

    configvars_file = os.path.join(confroot, '_configvars.conf')

    # Parse cache file and check verison.
    expected_checksum = shared.file.md5.compute_file_md5(
        configvars_file).hexdigest()
    try:
        saved_vals = configbuilder.parse_key_value_file(
            cache_data.splitlines(True))
        checksum = saved_vals.pop(ipconf.CONF_VERSION_CHECKSUM_VAR)
    except (ValueError, KeyError) as e:
        logging.error('The given file is not a valid ipconf cache file.\n%s',
                      e)
        return 1

    if not force and checksum != expected_checksum:
        logging.error('Cache file from Perforce is for a different version '
                      'of this product configuration.\n'
                      'cache file %s: %s\n'
                      '_configvars.conf md5 checksum:      %s\n',
                      ipconf.CONF_VERSION_CHECKSUM_VAR, checksum,
                      expected_checksum)
        return 1

    # Need to convert saved_vals to formatted vals.
    configvars = configbuilder.parse_configvars_conf(configvars_file)
    try:
        formatted_vals = configbuilder.format_vals(
            configvars, saved_vals, use_defaults=True,
            ignore_extra=bool(force))
    except configbuilder.InvalidValueError, e:
        logging.error('Incompatible cache file: %s', e)
        return 1

    configbuilder.build_files(confroot, formatted_vals)

    return 0


def main():

    option_parser = ipconf.get_option_parser(
        usage='%prog [options] <target>',
        epilog='<target> is either the product environment (e.g. '
        'prod-whiskey-app) or a complete Perforce path to the config '
        'data.')

    option_parser.add_option(
        '-f', '--force', action='store_true', dest='force',
        help='Try to continue if config data does not match current version.')
    option_parser.add_option(
        '-r', '--revision', action='store', dest='revision',
        help='Pull specific revision from Perforce.')
    option_parser.add_option(
        '--qa-mode', action='store_true', dest='qa_mode',
        help='Interpret <target> as a path to a local file.')

    (options, args) = option_parser.parse_args()

    ipconf.configure_logging(options)

    if len(args) != 1:
        logging.error('Need an environment name or a Perforce path')
        return 1

    target = args[0]

    if not options.qa_mode:

        paths = _get_perforce_paths(target, options)

        if paths:
            logging.info('Fetching configuration info from %s', paths)
        else:
            logging.warn('Could not parse target. Not a valid environment '
                         'name.')

        data = None
        for path in paths:
            data = get_perforce_data(path)
            if data:
                logging.info('Read %d bytes from %s.', len(data), path)
                break

        if not data:
            logging.error('Could not find config data in Perforce. Tried %s\n',
                          paths)
            return 1

    else:
        # QA Mode.
        if os.path.exists(target):
            logging.info('In QA mode, using %s for config data.' %
                         (target,))
            with open(target) as f:
                data = f.read()
        else:
            logging.error('In QA mode, but %s is not a local file.', target)
            return 1

    return build_config_files(options.confroot, data, force=options.force)

if __name__ == '__main__':

    sys.exit(main())
