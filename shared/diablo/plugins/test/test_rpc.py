"""Unittest for RPCPlugin and rpc_method decorator.

:Authors: vkuznets
$Id: //prod/main/_is/shared/python/diablo/plugins/test/test_rpc.py#9 $
"""

import logging
import unittest2 as unittest

from shared.diablo.decorator_registrar import DecoratorRegistrar
from shared.diablo.plugins import rpc


# Setup logging
LOG_LEVEL = logging.INFO
logging.basicConfig(level=LOG_LEVEL)

RPC_PORT = 34567
SECTION_NAME = 'diablo'
CONFIG = {'%s.rpc_server_port' % (SECTION_NAME,): RPC_PORT,}


class FastRPCServerMock(object):
    """Mock for the FastRPCServer."""

    def __init__(self, root_object, *args, **kwargs):
        self.root = root_object
        self.daemon = True

    def start(self):
        pass

    def kill(self):
        pass

    def __getattr__(self, name):
        return getattr(self.root, name)



class TestRPCPlugin(unittest.TestCase):
    """Test the plugin class functionality and the rpc_method decorator.
    """

    @classmethod
    def setUpClass(cls):
        cls.orig_server = rpc.FastRPCServer
        rpc.FastRPCServer = FastRPCServerMock

    @classmethod
    def tearDownClass(cls):
        rpc.FastRPCServer = cls.orig_server

    def test_01(self):
        """Test plugin get_status method."""

        plug = rpc.RPCPlugin(MockDaemon(), CONFIG, SECTION_NAME)

        plug.startup()
        status = plug.get_status()
        self.assertIn('port', status)
        self.assertIn('methods', status)
        plug.shutdown()

    def test_02(self):
        """Test basic RPC calls."""

        plug = rpc.RPCPlugin(MockDaemon(), CONFIG, SECTION_NAME)
        plug.startup()
        self.assertEqual(plug.server.hello(), MockDaemon.app_name)
        self.assertEqual(plug.server.echo('foo'), 'foo')
        self.assertRaises(AttributeError, lambda: plug.server.rpc_echo)
        plug.shutdown()

    def test_03(self):
        """Test plugin port is configured properly."""

        plug = rpc.RPCPlugin(MockDaemon(), CONFIG, SECTION_NAME)

        plug.startup()
        status = plug.get_status()
        expected_port = CONFIG['%s.rpc_server_port' % (SECTION_NAME,)]
        self.assertEqual(expected_port, status['port'])
        plug.shutdown()


class MockDaemon(object):
    """Mock Diablo daemon object."""

    __metaclass__ = DecoratorRegistrar

    app_name = 'mock_daemon'
    node_id = 0
    _plugins = []

    def stop_daemon(self, reason=None):
        pass

    @rpc.rpc_method
    def hello(self):
        return self.app_name

    @rpc.rpc_method(name='echo')
    def rpc_echo(self, param):
        return param


if __name__ == '__main__':
    unittest.main()
