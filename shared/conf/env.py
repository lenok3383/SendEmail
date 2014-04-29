"""Common functions to deal with environment variables.

:Status: $Id: //prod/main/_is/shared/python/conf/env.py#4 $
:Authors: lromanov
"""

import os


class ConfRootError(Exception):
    """Error raised when the CONFROOT environment variable is not set."""
    def __init__(self, msg='CONFROOT environment variable must be set.'):
        Exception.__init__(self, msg)


class ProdRootError(Exception):
    """Error raised when the PRODROOT environment variable is not set."""
    def __init__(self, msg='PRODROOT environment variable must be set.'):
        Exception.__init__(self, msg)


def get_conf_root():
    """Return the path to the CONFROOT, else raises an exception.

    :Parameters:
    - None

    :Returns:
    - Path to the CONFROOT.
    """
    conf_root = os.environ.get('CONFROOT')
    if conf_root is None:
        raise ConfRootError()
    return conf_root


def get_prod_root():
    """Return the path to the PRODROOT, else raises an exception.

    :Parameters:
    - None

    :Returns:
    - Path to the PRODROOT.
    """
    prod_root = os.environ.get('PRODROOT')
    if prod_root is None:
        raise ProdRootError()
    return prod_root
