# Copyright (c) 2011-2013 Cisco IronPort Systems LLC.
# All rights reserved.
# Unauthorized redistribution prohibited.

"""Implements logging configuration methods.

:Status: $Id: //prod/main/_is/shared/python/avc/utils/logutils.py#2 $
:Author: ivlesnik
:Last Modified By: $Author: ivlesnik $
:Date: $Date: 2013/12/20 $
"""

import os
import sys
import logging

from logging.config import fileConfig
from shared.conf.env import get_conf_root


# Constants.
DEFAULT_LOG_FORMAT_STRING = '%(asctime)s [%(name)s %(filename)s ' \
                            '(%(lineno)d)] %(levelname)s: %(message)s'
DEFAULT_LOG_CONFIG = 'log.conf'
LOG_LEVELS = [
    logging.CRITICAL,
    logging.FATAL,
    logging.ERROR,
    logging.WARNING,
    logging.INFO,
    logging.DEBUG,
]
CLI_TO_LOG_LEVELS = {
    0: logging.ERROR,
    1: logging.WARNING,
    2: logging.INFO,
    3: logging.DEBUG,
}


class LogutilsException(Exception):

    """General Logutils exception class."""

    pass


def get_logger(name=None):
    """Get the logger instance.

    :Parameters:
        `name`: logger name.

    :Return:
        logger instance.
    """

    return logging.getLogger(name)

def config_std_log(stream=sys.stdout, name=None,
                   remove_existing_handlers=False,
                   format_string=DEFAULT_LOG_FORMAT_STRING):
    """Configure logging to use stdout or stderr.

    :Parameters:
        `stream`: stream to log messages to.
        `name`: logger name.
        `remove_existing_handlers`: disable all existing log handlers.
    """

    logger = get_logger(name)

    if remove_existing_handlers:
        # Remove all logger handlers.
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
    else:
        # Don't add an existing handler.
        for handler in logger.handlers:
            if (isinstance(handler, logging.StreamHandler) and
                handler.stream is stream):
                return

    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter(format_string))

    logger.addHandler(handler)

def config_log_from_file(log_conf):
    """Configure logging using configuration file.

    :Parameters:
        `log_conf`: config file name or full path.
                    When full path specified, then use it.
                    Otherwise look for the file name in $CONFROOT.
    :Raises:
        LogutilsException: when cannot read log.conf
    """

    # Check the log file access by absolute path.
    if os.path.isabs(log_conf):
        if not os.access(log_conf, os.R_OK):
            raise LogutilsException('Could not read log configuration ' \
                'file %s.' % (log_conf,))
    # If log_conf is base file name, then look for it in $CONFROOT.
    else:
        confroot_filename = os.path.join(get_conf_root(), log_conf)
        if os.access(confroot_filename, os.R_OK):
            log_conf = confroot_filename
        else:
            raise LogutilsException('Could not read log configuration file.\n'
                'Tried looking in $CONFROOT (%s).' % (confroot_filename,))

    try:
        fileConfig(log_conf, disable_existing_loggers=False)
    except KeyError as err:
        # fileConfig exception handling is quite poor.  We're compensating here
        # by catching the most common one (a KeyError on a log pointing to a
        # file that it cannot open) and providing a suggestion for how to fix.
        raise LogutilsException('Cannot load log config from file: %s. '
                        'Check permissions on all files pointed to in the '
                        'log config.' % str(err))
    except (IOError, OSError) as err:
         raise LogutilsException(
            'Cannot write to log file. Reason: %s' % (err,))

def setup_logger(verbosity=None, name=None, log_conf=None,
                 stream=None, remove_existing_handlers=False,
                 format_string=DEFAULT_LOG_FORMAT_STRING):
    """Set up logging facility.

    Uses log.conf if stream is not specified.

    :Parameters:
        `verbosity`: numeric verbosity level.
        `name`: logger name.
        `log_conf`: config file name or full path.
        `stream`: stream to log messages to.
        `remove_existing_handlers`: remove existing log handlers
            (for streams only).
    :Return:
        logger instance.
    """

    if log_conf:
        config_log_from_file(log_conf)
    if stream:
        config_std_log(stream, name, remove_existing_handlers, format_string)
    if not log_conf and not stream:
        config_std_log(sys.stderr, name, remove_existing_handlers,
                       format_string)

    if verbosity is not None:
        set_log_level(verbosity, name)

    return get_logger(name)

def set_log_level(verbosity, name=None):
    """Set log level based on verbosity.

    If it doesn't match to logging levels, then logging.DEBUG will be used.

    :Parameters:
        `verbosity`: numeric verbosity level. It can be either one of the
            standard logging levels (logging.INFO etc.) or a product-based CLI
            verbosity level (0, 1, 2, 3)
        `name`: logger name.
    """

    if verbosity in LOG_LEVELS:
        log_level = verbosity
    elif verbosity in CLI_TO_LOG_LEVELS:
        log_level = CLI_TO_LOG_LEVELS[verbosity]
    else:
        log_level = logging.DEBUG

    logger = get_logger(name)
    logger.setLevel(log_level)

def log_exception(log, exc, verbosity, message=None):
    """Logs the appropriate error message for the given verbosity level.

    If verbosity is in range 0-3, then a regular error will be logged.
    If verbosity is 500, then the exception will be logged (developers mode).

    :Parameters:
        -`log`: logger instance
        -`exc`: exception instance
        -`verbosity`: level of verbosity
        -`message`: optional error message to be used instead of exception
                    message for verbosity != 500.
    """

    if verbosity == 500:
        log.exception(exc)
    else:
        if message is None:
            log.error(str(exc))
        else:
            log.error(message)
