import socket
import threading
import unittest2 as unittest
import xmlrpclib

from SimpleXMLRPCServer import SimpleXMLRPCServer

import shared.monitor.storage_types.xmlrpc_counter_storage


class XMLRPCCounterTest(unittest.TestCase):

    def setUp(self):

        def post_data(key, value, name):
            if self.error:
                return False
            self.received_data.append([key, value, name])
            return True

        self.error = False
        self.received_data = []
        self.server = SimpleXMLRPCServer(('localhost', 8888), logRequests=False)
        self.server.allow_reuse_address = True
        self.server.register_function(post_data, 'post_data')
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.start()
        self.addCleanup(self.server.server_close)

    def tearDown(self):
        self.server.shutdown()
        self.server_thread.join()

    def test_storage(self):
        storage = shared.monitor.storage_types.xmlrpc_counter_storage.XMLRPCCounterStorage(
            'http://localhost:8888', remote_method='post_data', max_buffer_size=2)
        storage.store('test_key1', 30, 'test_counter')
        self.assertSequenceEqual(['test_key1', 30, 'test_counter'],
                                 self.received_data[-1])
        storage.store('test_key2', 20, 'test_counter')
        self.assertSequenceEqual(['test_key2', 20, 'test_counter'],
                                 self.received_data[-1])

    def test_storage_failure(self):
        storage = shared.monitor.storage_types.xmlrpc_counter_storage.XMLRPCCounterStorage(
            'http://localhost:8888', remote_method='post_data', max_buffer_size=2)
        self.error = True
        with self.assertRaises(shared.monitor.storage_types.StorageError):
            storage.store('test_key1', 30, 'test_counter')
        self.assertSequenceEqual([], self.received_data)

        with self.assertRaises(shared.monitor.storage_types.StorageError):
            storage.store('test_key2', 20, 'test_counter')
        self.assertSequenceEqual([], self.received_data)

        self.error = False
        storage.store('test_key3', 10, 'test_counter')
        self.assertSequenceEqual([['test_key2', 20, 'test_counter'],
                                  ['test_key3', 10, 'test_counter']],
                                  self.received_data)

    def test_socket_failure(self):
        storage = shared.monitor.storage_types.xmlrpc_counter_storage.XMLRPCCounterStorage(
            'http://localhost:9999', remote_method='post_data', max_buffer_size=2)
        self.received_data = []
        with self.assertRaises(shared.monitor.storage_types.StorageError):
            success = storage.store('test_key1', 30, 'test_counter')


if __name__ == '__main__':
    unittest.main()
