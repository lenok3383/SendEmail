"""Module with logging formatters.

:Status: $Id: //prod/main/_is/shared/python/logging/formatters.py#6 $
:Authors: jwescott, aflury, kylev
"""
import logging
import os
import sys


_app_name = None

def set_app_name(app_name):
    """Set the application name for the application-aware logging handlers."""
    global _app_name
    _app_name = app_name


def get_app_name():
    """Get the application name for the application-aware logging handlers.

    Defaults to the basename (without extension) of the current script.
    """
    if _app_name is None:
        return os.path.splitext(os.path.basename(sys.argv[0]))[0]
    else:
        return _app_name


class AppFormatter(logging.Formatter):
    """Extended Formatter with application tag."""

    def __init__(self, fmt=None, datefmt=None):
        """AppFormatter initialization.

        Application name is added to logging format.

        :Parameters:
        - fmt: logging format.
        - datefmt: datetime format.
        """
        logging.Formatter.__init__(self, fmt, datefmt)
        self._fmt = '%s: %s' % (get_app_name(), self._fmt)

