"""
Tests for shared FastRPC package

:Authors: mskotyn, vkuznets
"""

import cPickle
import logging
import socket

import unittest2 as unittest

from shared.rpc import RPCError, RPCServerUnreachable

from shared.rpc.blocking_server import FastRPCServer
from shared.rpc.blocking_server import _FastRPCServerChannel
from shared.rpc.client import FastRPCClient
from shared.rpc.client import FastRPCProxy

from shared.rpc.test import FastRPCServerSocketMock
from shared.rpc.test import FastRPCClientSocketMock
from shared.rpc.test import FastRPCServerMock
from shared.rpc.test import FastRPCClientMock
from shared.rpc.test import TestRoot

logging.basicConfig(level=logging.CRITICAL)


class TestFastRPCProxy(unittest.TestCase):

    """Unittest for FastRPCProxy."""

    def setUp(self):
        self.conn_mock = FastRPCClientMock()
        self.proxy = FastRPCProxy(self.conn_mock)

    def test_method_call(self):
        """Test for method call."""
        self.proxy.some_method('arg')
        self.assertEqual(self.conn_mock.called, [(('some_method',), ('arg',))])

    def test_attr(self):
        """Test for call of particular object method."""
        self.proxy.some_attr.some_method('arg')
        self.assertEqual(self.conn_mock.called,
                         [(('some_attr', 'some_method'), ('arg',))])


class TestFastRPCClient(unittest.TestCase):

    """Unittest to test FastRPCClient class"""

    def setUp(self):
        self.old_socket_function = socket.socket
        socket.socket = FastRPCClientSocketMock
        self.clnt = FastRPCClient(('localhost', 0))
        socket.socket.FAIL_ON_CONNECT = False

    def tearDown(self):
        self.clnt.close()
        socket.socket = self.old_socket_function

    def test_rpc_client(self):
        """Test FastRPCClient is able to understand server protocol"""
        self.assertEqual(self.clnt.get_proxy().add(1, 2, 3), 6)

    def test_rpc_exceptions(self):
        """Test FastRPCClient raises correct exception when could"""\
        """ not connect to server"""
        socket.socket.FAIL_ON_CONNECT = True
        self.assertRaises(RPCServerUnreachable, self.clnt.get_proxy().add,
                                                             ((1, 2, 3),))


class TestFastRPCServerChannel(unittest.TestCase):

    """Unittest for FastRPCServerChannel."""

    def test_rpc_server_channel(self):
        """Test _FastRPCServerChannel object"""
        root = TestRoot()
        server_mock = FastRPCServerMock(root)
        conn_mock = FastRPCServerSocketMock(data=self._pack(('add',), (1, 2, 3)))
        chan = _FastRPCServerChannel(server_mock, conn_mock, ('localhost', 0))
        chan.run()
        chan.kill_event.set()
        self.assertEqual(self._unpack(conn_mock.send_data[0]), (0, None, 6))

    def _pack(self, path, params):
        packet = cPickle.dumps((0, path, params))
        data = []
        data.append('%08x' % (len(packet),))
        data.append(packet)
        return ''.join(data)

    def _unpack(self, data):
        return cPickle.loads(data[8:])


class TestFastRPCClientServer(unittest.TestCase):

    """Unittest to test real client/server pair"""

    RPC_PORT = 13990

    def setUp(self):
        self.log = logging.getLogger('RPC test')
        TestFastRPCClientServer.RPC_PORT += 1
        self.addr = ('', TestFastRPCClientServer.RPC_PORT)
        self.server = FastRPCServer(TestRoot(), self.addr)
        self.server.start()
        self.client = FastRPCClient(self.addr, True)
        self.remote_root = self.client.get_proxy()

    def tearDown(self):
        self.client.close()
        self.server.kill()
        self.server.join()

    def test_remote_method_calls(self):
        """
        Test remote method calls
        """
        self.assertEqual(self.remote_root.add(1, 2, 3, 4), 10)
        self.assertEqual(self.remote_root.mul(3, 5), 15)

    def test_exceptions(self):
        """
        Test calling nonexistent method raises RPCError
        """
        self.assertRaises(RPCError, self.remote_root.notexist, (1, 2))


if __name__ == "__main__":
    unittest.main()
