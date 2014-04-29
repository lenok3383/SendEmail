"""Unittest for DiabloPlugin base class.

:Authors: scottrwi
$Id: //prod/main/_is/shared/python/diablo/plugins/test/test_plugin_base.py#4 $
"""

import errno
import logging
import operator
import os
import re
import socket
import time
import unittest2 as unittest

from shared.diablo.plugins import DiabloPlugin, generate_port, DEV_PORT_OFFSET

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)


class TestPlugin(unittest.TestCase):
    """Test DiabloBase class
    """

    def setUp(self):
        self.plug = DiabloPlugin(MockDaemon(), {}, '')
        self.thread_running = False

    def quick_exit(self):
        pass

    def test_thread_quick_exit(self):
        self.plug.start_in_thread('quick', self.quick_exit)
        self.plug.join_threads()
        self.assertTrue(self.plug.daemon_obj.called_stop)

    def raise_exception(self):
        raise Exception()

    def test_thread_exception(self):
        self.plug.start_in_thread('exception', self.raise_exception)
        self.plug.join_threads()
        self.assertTrue(self.plug.daemon_obj.called_stop)

    def short_run(self):
        self.thread_running = True
        time.sleep(0.1)
        self.thread_running = False

    def test_daemonized(self):
        self.plug.start_in_thread('short', self.short_run,
                                  daemonize=True)
        # Make sure thread gets chance to run.
        time.sleep(0.001)
        self.plug.join_threads()
        self.assertTrue(self.thread_running)


class TestGeneratePort(unittest.TestCase):

    def setUp(self):
        self.base_port = 40
        if DEV_PORT_OFFSET in os.environ:
            self.addCleanup(operator.setitem, os.environ, DEV_PORT_OFFSET,
                            os.environ[DEV_PORT_OFFSET])

    def test_generate_port(self):
        os.environ['DIABLO_DEV_PORT_OFFSET'] = '50000'
        port = generate_port(self.base_port, 2, 'test')

        self.assertTrue(re.compile('5\d\d42').match(str(port)))
        self.assertTrue(str(port).endswith('42'))

        del os.environ['DIABLO_DEV_PORT_OFFSET']

    def test_generate_port_too_big(self):
        os.environ['DIABLO_DEV_PORT_OFFSET'] = '500000'
        self.assertRaises(ValueError, generate_port, self.base_port, 2, 'test')

        del os.environ['DIABLO_DEV_PORT_OFFSET']


class MockDaemon(object):
    def __init__(self):
        self.called_stop = False

    def stop_daemon(self, reason):
        self.called_stop = True

    def get_status(self):
        return {'app_name': 'test',
                'node_id': 2}


if __name__ == '__main__':
    unittest.main()

