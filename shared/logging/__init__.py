"""Logging package.

:Status: $Id: //prod/main/_is/shared/python/logging/__init__.py#6 $
:Authors: vscherb
"""
import logging.config
import os

from shared.conf import env

class LoggingReinitError(Exception):
    def __init__(self):
        Exception.__init__(self, "Logging system is already initialized")

__initialized = False

def configure_logging(conf_filename='log.conf', ignore_reinit=False):
    """Configure the logging system from a config file.

    Read the logging configuration from a configparser-format file and
    configure the logging system.

    :param conf_filename: Log config file pathname.  Default: 'log.conf'
    :param ignore_reinit: boolean, simply ignore a re-initialization atempt

    :raises: LoggingReinitError if the logging system is already initialized
    """
    global __initialized
    if is_logging_initialized():
        if ignore_reinit:
            return
        raise LoggingReinitError()

    (head_pathname, tail_pathname) = os.path.split(conf_filename)
    if not head_pathname:
        conf_filename = os.path.join(env.get_conf_root(), tail_pathname)
    logging.config.fileConfig(conf_filename)
    logging.getLogger(__name__).debug("Logging system initialized.")
    __initialized = True

def is_logging_initialized():
    """
    Return True if logging system is already initialized
    """
    return __initialized

