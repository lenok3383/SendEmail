#!/usr/bin/env python
"""IPConf - List config versions in Perforce

:Author: bwhitela
:Status: $Id: //prod/main/_is/shared/python/conf/tools/ipconf_changes.py#2 $
"""
import logging
import sys
import time

import shared.scm.perforce
from shared.conf.tools import ipconf


def get_perforce_change_data(path, options):
    """Reads data from Perforce for the given Perforce path.

    :Parameters:
        - `path`: A path in Perforce.
        - `options`: Options object containing options parsed from command
          line.

    :Returns: Change data related to `path` in the data structure defined by
        Perforce.changes (in shared.scm.perforce)
    """
    with shared.scm.perforce.Perforce() as p:
        logging.info('Getting Perforce changes for path %s', path)
        return p.changes(path, timeout=options.timeout,
                         max_results=options.max_results)


def format_change_data(change_data):
    """Format a printable string for the change data.

    :Parameters:
        - `change_data`: A data structure defined by Perforce.changes
          (in shared.scm.perforce)

    :Returns: A printable string based on the change_data.
    """
    change_strs = []
    for change in change_data:
        user = change['user'].split('@')[0]
        change_num = change['change_number']
        date = time.strftime('%Y/%m/%d', time.localtime(change['change_date']))
        description = change['description']
        change_str = 'User %s submitted %d on %s.\nDescription: %s\n' % (user,
                     change_num, date, description)
        change_strs.append(change_str)
    return '\n'.join(change_strs)


def main():

    option_parser = ipconf.get_option_parser(
        usage='%prog [options] <target>',
        epilog='<target> is either the product environment (e.g. '
        'prod-whiskey-app) or a complete Perforce path to the config '
        'data.')

    option_parser.add_option(
        '-m', '--max', action='store', dest='max_results', default=5,
        help='Maximum number of versions to show. 0 shows all. Default is 5.')
    option_parser.add_option(
        '-t', '--timeout', action='store', dest='timeout', default=10,
        help='Specify a timeout for Perforce (seconds). Default is 10 seconds.')

    (options, args) = option_parser.parse_args()

    ipconf.configure_logging(options)

    if len(args) != 1:
        logging.error('Need an environment name or a Perforce path')
        return 1

    target = args[0]
    paths = ipconf.get_perforce_paths(target, options)
    if paths:
        logging.info('Fetching configuration version info from %s', paths)
    else:
        logging.warn('Could not parse target. Not a valid environment '
                     'name.')

    for path in paths:
        change_data = get_perforce_change_data(path, options)
        if change_data:
            print 'Change data for file %s:\n' % (path,)
            print format_change_data(change_data)

    return 0


if __name__ == '__main__':

    sys.exit(main())
