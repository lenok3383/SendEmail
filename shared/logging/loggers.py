"""Module with logging handlers.

:Status: $Id: //prod/main/_is/shared/python/logging/loggers.py#7 $
:Authors: jwescott, aflury, kylev, ohmelevs
"""

import copy
import errno
import logging
import logging.handlers
import os
import socket
import time

from shared.logging import formatters


class AppRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """A Rotating File Handler that computes the log name from the app name.

    If the application name is not given via a call to ``set_app_name()``, we
    base the log name on the script that was executed. For example ``foo.py``
    results in a ``foo.log`` in the specified directory.
    """

    def __init__(self, file_dir, *args, **kwargs):
        logging.handlers.RotatingFileHandler.__init__(self,
            os.path.join(file_dir, formatters.get_app_name() + '.log'),
            *args, **kwargs)


class SafeSysLogHandler(logging.handlers.SysLogHandler):
    """SafeSysLogHandler class."""

    def __init__(self, address='/dev/log',
                 facility=logging.handlers.SysLogHandler.LOG_LOCAL6):
        """We override the default SysLogHandler to forcefully split up long
        log messages and handle exception traces cleanly."""

        logging.handlers.SysLogHandler.__init__(self, address, facility)

    def emit(self, record):
        """Emit a record.

        The record is formatted, and then sent to the syslog server.
        First we split up long records into _MSG_MAX_LEN size messages.
        We also control the rate at which we send messages to the socket. It
        can happen that the syslog daemon hasn't read off the socket and
        there's no buffer space for new messages. In that case, we do a
        back-off wait with increments of 0.1 sec ten times. That works out to
        about 1.0 second.
        """
        prior = self.encodePriority(self.facility, record.levelname.lower())
        for msg in self.format(record):
            msg = self.log_format_string % (prior, msg)
            sent = False
            timeout = 0.1
            while not sent:
                try:
                    if self.unixsocket:
                        try:
                            self.socket.send(msg)
                        except socket.error, e:
                            if e[0] == errno.ENOBUFS:
                                if timeout < 1.0:
                                    time.sleep(timeout)
                                    timeout += 0.1
                                    continue
                                else:
                                    raise
                            else:
                                self._connect_unixsocket(self.address)
                                self.socket.send(msg)
                    else:
                        self.socket.sendto(msg, self.address)
                    sent = True
                except:
                    self.handleError(record)
                    break

    def format(self, record):
        """Formats the specified record.  Overrides default format method
        to handle with long messages and tracebacks properly.

        :Parameters:
        - record: Log record.

        :Returns:
        A list of formatted messages as text.
        """
        exc_text = list()
        if self.formatter:
            fmt = self.formatter
        else:
            fmt = logging._defaultFormatter

        record.message = ''
        if '%(asctime)' in fmt._fmt:
            record.asctime = fmt.formatTime(record, fmt.datefmt)
        prefix = fmt._fmt % record.__dict__

        if record.exc_info:
            exc_text = [prefix + line for line in
                        _split_lines(fmt.formatException(record.exc_info),
                                     len(prefix))]

        # Skip standard exc_info handling
        record.exc_info = None
        record.exc_text = None

        formatted = [fmt.format(rec) for rec in _split_record(record,
                                                              len(prefix))]
        formatted.extend(exc_text)
        return formatted


# syslog message length limit [1024]
# - max length of mandatory syslog headers (time, hostname) [74]
# = 950
_MSG_MAX_LEN = 950

_MSG_MAX_LINES = 100

def _split_record(record, prefix_len):
    """Formats a log record."""
    lines = _split_lines(record.getMessage(), prefix_len)
    if len(lines) == 1:
        return [record]
    ret = []
    for l in lines:
        r = copy.copy(record)
        r.msg = l
        r.args = None
        ret.append(r)
    return ret


def _split_lines(msg, prefix_len):
    """Splits given message by maximum lines count and maximum line width."""
    max_len = _MSG_MAX_LEN - prefix_len
    raw_lines = msg.splitlines()
    lines = []
    line_cnt = 0
    for line in raw_lines:
        if len(line) > max_len:
            while line:
                lines.append(line[:max_len])
                line = line[max_len:]
                line_cnt += 1
                if line_cnt >= _MSG_MAX_LINES:
                    break
        else:
            lines.append(line)
            line_cnt += 1
        if line_cnt >= _MSG_MAX_LINES:
            break
    return lines

