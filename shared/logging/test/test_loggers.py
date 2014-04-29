"""Unit tests for logging package.

:Status: $Id: //prod/main/_is/shared/python/logging/test/test_loggers.py#7 $
:Authors: ohmelevs
"""

import errno
import logging
import logging.config
import os
import shutil
import socket
import tempfile
import unittest2

from shared.logging import formatters


test_conf = """
[loggers]
keys=root,app,app_file,sys_log

[handlers]
keys=app_handler,app_file_handler,safe_syslog_handler

[formatters]
keys=test_formatter,app_formatter

[logger_root]
level=NOTSET
handlers=

[logger_app]
level=NOTSET
handlers=app_handler
qualname=app

[logger_app_file]
level=NOTSET
handlers=app_file_handler
qualname=app_file

[logger_sys_log]
level=NOTSET
handlers=safe_syslog_handler
qualname=sys_log

[handler_app_handler]
class=FileHandler
formatter=app_formatter
args=('%s',)

[handler_app_file_handler]
class=shared.logging.loggers.AppRotatingFileHandler
formatter=test_formatter
args=('%s',)

[handler_safe_syslog_handler]
class=shared.logging.loggers.SafeSysLogHandler
args=('/dev/log', handlers.SysLogHandler.LOG_LOCAL6)
formatter=test_formatter

[formatter_test_formatter]
format=test: %%(asctime)s %%(levelname)s [%%(name)s] %%(message)s
datefmt=

[formatter_app_formatter]
class=shared.logging.formatters.AppFormatter
format=%%(levelname)s [%%(name)s] %%(message)s
"""


TEST_APP_NAME = 'loggers_test'


class TestLoggers(unittest2.TestCase):

    def setUp(self):
        self._create_conf_file()
        self.addCleanup(self._clean_up)
        formatters.set_app_name(TEST_APP_NAME)
        logging.config.fileConfig(self._conf_name)

    def tearDown(self):
        formatters.set_app_name(None)

    def test_app(self):
        logging.getLogger('app').info('some info')
        self.assertTrue(self._is_str_in_file('some info'))
        self.assertTrue(self._is_str_in_file('INFO'))
        self.assertTrue(self._is_str_in_file(TEST_APP_NAME))

    def test_app_rotating_file1(self):
        logging.getLogger('app_file').error('some error')
        self.assertTrue(self._is_str_in_file('some error'))
        self.assertTrue(self._is_str_in_file('ERROR'))

    def test_app_rotating_file2(self):
        logging.getLogger('app_file').info('some info')
        self.assertTrue(self._is_str_in_file('some info'))
        self.assertTrue(self._is_str_in_file('INFO'))

    def test_syslog1(self):
        socket = SocketMock(self._log_path)
        logging.getLogger('sys_log').handlers[0].socket = socket
        logging.getLogger('sys_log').error('some error')
        self.assertTrue(self._is_str_in_file('some error'))
        self.assertTrue(self._is_str_in_file('ERROR'))
        self.assertEqual(socket.send_count, 1)

    def test_syslog2(self):
        socket = SocketMock(self._log_path)
        logging.getLogger('sys_log').handlers[0].socket = socket
        logging.getLogger('sys_log').info('some info')
        self.assertTrue(self._is_str_in_file('some info'))
        self.assertTrue(self._is_str_in_file('INFO'))
        self.assertEqual(socket.send_count, 1)

    def test_syslog3(self):
        socket = SocketMock(self._log_path)
        logging.getLogger('sys_log').handlers[0].socket = socket
        logging.getLogger('sys_log').info('s' * 1025)
        self.assertEqual(socket.send_count, 2)

    def test_syslog4(self):
        socket = SocketMock(self._log_path)
        logging.getLogger('sys_log').handlers[0].socket = socket
        logging.getLogger('sys_log').info('s' * 2100)
        self.assertEqual(socket.send_count, 3)

    def test_syslog5(self):
        socket = SocketMock(self._log_path, True)
        logging.getLogger('sys_log').handlers[0].socket = socket
        logging.getLogger('sys_log').info('some info')
        self.assertTrue(self._is_str_in_file('some info'))
        self.assertTrue(self._is_str_in_file('INFO'))

    def _is_str_in_file(self, string):
        with open(self._log_path, 'r') as f:
            for l in f.readlines():
                if l.find(string) != -1:
                    return True
            return False

    def _create_conf_file(self):
        self._log_dir = tempfile.mkdtemp()
        tmp, self._conf_name = tempfile.mkstemp(dir=self._log_dir)
        os.close(tmp)
        self._log_path = os.path.join(self._log_dir,
                                      '%s.log' % (TEST_APP_NAME,))
        with open(self._conf_name, 'w') as f:
            f.write(test_conf % (self._log_path, self._log_dir))

    def _clean_up(self):
        shutil.rmtree(self._log_dir, ignore_errors=True)


class SocketMock:
    """Mock of the socket to redirect log messages from syslog
    to a test log file."""
    def __init__(self, log_name, raise_exc=False):
        self.logname = log_name
        self.send_count = 0
        self.raise_exc = raise_exc
    def sendto(self, msg, addr):
        self.send(msg)
    def send(self, msg):
        if self.raise_exc:
            #exception is raised once to test the retry logic.
            self.raise_exc = False
            raise socket.error(errno.ENOBUFS)
        with open(self.logname, 'w') as f:
            f.write(msg)
        self.send_count += 1
    def close(self):
        pass


if __name__ == '__main__':
    unittest2.main()

