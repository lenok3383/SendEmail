import threading
import time
import unittest2 as unittest

from shared.rpc.blocking_server import FastRPCServer

import shared.monitor.storage_types.fastrpc_counter_storage


class FastRPCCounterTest(unittest.TestCase):

    RPC_PORT = 13990

    def setUp(self):
        self.error = False
        self.received_data = []
        FastRPCCounterTest.RPC_PORT += 1
        self.test_methods = TestMethods()
        self.server = FastRPCServer(self.test_methods, ('localhost', FastRPCCounterTest.RPC_PORT))
        self.server.start()
        self.storage = shared.monitor.storage_types.fastrpc_counter_storage.FastRPCCounterStorage(
            'localhost', FastRPCCounterTest.RPC_PORT, remote_method='post_data', max_buffer_size=2)

    def tearDown(self):
        self.server.kill()
        self.server.join()

    def test_storage(self):
        self.storage.store('test_key1', 30, 'test_counter')
        self.assertSequenceEqual(['test_key1', 30, 'test_counter'],
                                 self.test_methods.received_data[-1])
        self.storage.store('test_key2', 20, 'test_counter')
        self.assertSequenceEqual(['test_key2', 20, 'test_counter'],
                                 self.test_methods.received_data[-1])

    def test_storage_failure(self):
        self.test_methods.error = True
        with self.assertRaises(shared.monitor.storage_types.StorageError):
            self.storage.store('test_key1', 30, 'test_counter')
        self.assertSequenceEqual([], self.test_methods.received_data)

        with self.assertRaises(shared.monitor.storage_types.StorageError):
            self.storage.store('test_key2', 20, 'test_counter')
        self.assertSequenceEqual([], self.test_methods.received_data)

        self.test_methods.error = False
        self.storage.store('test_key3', 10, 'test_counter')
        self.assertSequenceEqual([['test_key2', 20, 'test_counter'],
                                  ['test_key3', 10, 'test_counter']],
                                  self.test_methods.received_data)

    def test_socket_failure(self):
        storage = shared.monitor.storage_types.fastrpc_counter_storage.FastRPCCounterStorage(
            'localhost', 19999, remote_method='post_data')
        self.test_methods.received_data = []
        with self.assertRaises(shared.monitor.storage_types.StorageError):
            success = storage.store('test_key1', 30, 'test_counter')


class TestMethods(object):
    """Methods to be used by the RPC server."""
    def __init__(self):
        self.error = False
        self.received_data = []

    def post_data(self, key, value, name):
        if self.error:
            return False
        self.received_data.append([key, value, name])
        return True


if __name__ == '__main__':
    unittest.main()
