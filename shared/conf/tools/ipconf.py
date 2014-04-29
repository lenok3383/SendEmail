#!/usr/bin/env python
"""IPConf - code shared among ipconf scripts.

:Author: duncan
:Status: $Id: //prod/main/_is/shared/python/conf/tools/ipconf.py#3 $
"""

import logging
import socket
import optparse
import os

import shared.conf.env

P4_TREE_ROOT = '//prod/main/_is/config'
CONF_VERSION_CHECKSUM_VAR = "ipconf_version_checksum"


class IPConfException(Exception):
    """IPConf Base Exception class."""


class InvalidOptionsError(Exception):
    """Invalid command line options were provided."""


def _guess_package_from_confroot(confroot):
    """Guess the name of the package we're configuring from the confroot."""
    # We assume confroot is /directory/<package>/etc (with or without
    # trailing slash).
    prodroot, etc = os.path.split(confroot.rstrip('/'))
    if etc != 'etc':
        raise InvalidOptionsError(
            'Cannot parse package name from confroot. You must specify the '
            '--package option.')
    _, package = os.path.split(prodroot)
    return package


def get_perforce_paths(environment, options):
    """Return perforce paths to search for configuration info.

    :Parameters:
        - `environment`: parameter given on the command line, generally a
          "<env>-<product>-<role>" tag or the actual perforce path.
        - `options`: an options object as returned by the option parser
          provided by `get_option_parser()`
        - `revision`: optionally, a specific reversion to pull from Perforce

    :Returns: Perforce paths in order of preference. Empty list if target is
        invalid. Single item list if a Perforce path is specified.
    """
    if environment.startswith('//'):
        return [environment]

    try:
        env, product, role = environment.split('-')
    except ValueError:
        return []

    package = options.package
    if package is None:
        package = _guess_package_from_confroot(options.confroot)

    paths = [
        '%s/host/%s/%s.cf' % (P4_TREE_ROOT, options.hostname, package),
        '%s/env/%s/%s/%s/%s.cf' % (P4_TREE_ROOT, env, product, role, package)]

    return paths


def get_option_parser(*args, **kwargs):
    """Parse common command line options.

    Also, sets sensible defaults.
    """
    option_parser = optparse.OptionParser(*args, **kwargs)

    option_parser.set_defaults(
        confroot=shared.conf.env.get_conf_root(),
        hostname=socket.getfqdn())

    option_parser.add_option(
        '-v', '--verbose', action='store_true', dest='verbose',
        help='Be more verbose.')
    option_parser.add_option(
        '-c', '--confroot', action='store', dest='confroot',
        help='Path to product configuration directory (default %default).')
    option_parser.add_option(
        '--package', action='store', dest='package',
        help='Name of product (by default parsed from confroot).')
    option_parser.add_option(
        '--hostname', action='store', dest='hostname',
        help='Specify hostname for this configuration (default %default).')

    return option_parser


def configure_logging(options):

    level = logging.ERROR
    if options.verbose:
        level = logging.DEBUG

    logging.basicConfig(level=level, format='%(message)s')


