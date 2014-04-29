import atexit
import os
import socket
import sqlite3
import tempfile
import time
import threading
import unittest2 as unittest
import xmlrpclib

from SimpleXMLRPCServer import SimpleXMLRPCServer

import shared.monitor.counter
import shared.monitor.counter_storage
import shared.db
import shared.db.utils
import shared.testing.case


class SQLiteCounterTest(unittest.TestCase):

    def setUp(self):
        _, db_path = tempfile.mkstemp()
        self.db_path = db_path
        self.config = {'counters.db_path': db_path,
                       'counters.flush_interval': 0.05}

        self.storage = shared.monitor.counter_storage.SQLiteCounterStorage(
                            self.db_path, clear_contents=False)

    def tearDown(self):
        os.unlink(self.db_path)

    def test_counter_storage_init(self):
        # Starts empty
        self.assertSequenceEqual(self._get_all_data(), [])

        # Insert some data
        self._store_data([('foo', 'foo', 1234, 1.0, 123456)])

        # Clear contents on this init
        shared.monitor.counter_storage.SQLiteCounterStorage(
            self.db_path, clear_contents=True)
        self.assertSequenceEqual(self._get_all_data(), [])

    def test_counter_storage_store(self):
        self.storage.store('test_key1', 30, 'test_counter')
        self.storage.store('test_key2', 20, 'test_counter')
        self.storage.store('test_key1', 10, 'test_counter')
        conn = sqlite3.connect(self.db_path)
        with conn:
            c = conn.execute(
                'SELECT key, name, process_id, value '
                'FROM counts ORDER BY key ASC')
            rows = c.fetchall()
        self.assertEqual(len(rows), 2)
        self.assertSequenceEqual(
            rows[0], [u'test_key1', u'test_counter', os.getpid(), 10])
        self.assertSequenceEqual(
            rows[1], [u'test_key2', u'test_counter', os.getpid(), 20])

    def test_aggregate_empty(self):
        # Started empty, should return no counts
        self.assertEqual(self.storage.aggregated_counts(), {})

    def test_aggregate_bad_suffix(self):
        db_data = [('key:badsuffix', 'name', 1, 1.0, 12345)]
        self._store_data(db_data)
        # Should be empty
        self.assertEqual(self.storage.aggregated_counts(), {})

    def test_aggregate_st(self):
        db_data = [('key:st', 'name', 1, 1.0, 12345),
                   ('key:st', 'name', 2, 1.0, 12345)]
        # Should be sum of the values
        expected = {'key:st': 2.0}
        self._store_data(db_data)
        self.assertEqual(self.storage.aggregated_counts(), expected)

    def test_aggregate_ts(self):
        db_data = [('key:ts', 'name', 1, 1, 12345),
                   ('key:ts', 'name', 2, 12345, 12345)]
        # Should be highest value
        expected = {'key:ts': 12345.0}
        self._store_data(db_data)
        self.assertEqual(self.storage.aggregated_counts(), expected)

    def test_aggregate_rate(self):
        db_data = [('key:r300', 'name', 1, 1, 12345),
                   ('key:r300', 'name', 2, 2, 12345)]
        # Should be average of values
        expected = {'key:r300': 1.5}
        self._store_data(db_data)
        self.assertEqual(self.storage.aggregated_counts(), expected)

    def _get_all_data(self):
        conn = sqlite3.connect(self.db_path)
        with conn:
            c = conn.execute('SELECT * FROM counts')
            rows = c.fetchall()
        return rows

    def _store_data(self, data):
        conn = sqlite3.connect(self.db_path)
        with conn:
            for row in data:
                conn.execute('INSERT OR REPLACE INTO counts('
                             'key, name, process_id, value, mtime'
                             ') values (?, ?, ?, ?, ?)', row)


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
        storage = shared.monitor.counter_storage.XMLRPCCounterStorage(
            'http://localhost:8888', remote_method='post_data', max_buffer_size=2)
        storage.store('test_key1', 30, 'test_counter')
        self.assertSequenceEqual(['test_key1', 30, 'test_counter'],
                                 self.received_data[-1])
        storage.store('test_key2', 20, 'test_counter')
        self.assertSequenceEqual(['test_key2', 20, 'test_counter'],
                                 self.received_data[-1])

    def test_storage_failure(self):
        storage = shared.monitor.counter_storage.XMLRPCCounterStorage(
            'http://localhost:8888', remote_method='post_data', max_buffer_size=2)
        self.error = True
        with self.assertRaises(shared.monitor.counter_storage.StorageError):
            storage.store('test_key1', 30, 'test_counter')
        self.assertSequenceEqual([], self.received_data)

        with self.assertRaises(shared.monitor.counter_storage.StorageError):
            storage.store('test_key2', 20, 'test_counter')
        self.assertSequenceEqual([], self.received_data)

        self.error = False
        storage.store('test_key3', 10, 'test_counter')
        self.assertSequenceEqual([['test_key2', 20, 'test_counter'],
                                  ['test_key3', 10, 'test_counter']],
                                  self.received_data)

    def test_socket_failure(self):
        storage = shared.monitor.counter_storage.XMLRPCCounterStorage(
            'http://localhost:9999', remote_method='post_data', max_buffer_size=2)
        self.received_data = []
        with self.assertRaises(shared.monitor.counter_storage.StorageError):
            success = storage.store('test_key1', 30, 'test_counter')


class MySQLCounterTest(shared.testing.case.TestCase):

    DB_POOL_NAME = 'counter_pool'
    NODE_NAME = 'test_node'

    def setUp(self):
        self.dbm = self.get_temp_database_manager()
        self.dbm.create_db(self.DB_POOL_NAME)
        self._rw_pool = shared.db.get_rw_pool(self.DB_POOL_NAME)
        self.storage = shared.monitor.counter_storage.MySQLCounterStorage(
            db_pool=self._rw_pool, node_name = self.NODE_NAME)

    @shared.db.utils.retry
    def _execute_sql(self, sql):
        """Simple SQL executing function."""
        with self._rw_pool.transaction() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def test_storage_init(self):
        """Check that storage is properly initialized."""
        sql = 'SHOW TABLES'
        all_tables = self._execute_sql(sql)
        self.assertEqual(all_tables, (('counters',),))
        sql = 'SELECT * FROM counters'
        table_data = self._execute_sql(sql)
        self.assertEqual(table_data, ())

    def test_single_store(self):
        """Check that a single store operation works properly."""
        counter_name = 'test_counter'
        key = 'test_counter:dyn'
        value = 10
        self.storage.store(key, value, counter_name)

        sql = """SELECT counter_name, node_name, value, mby, hostname, pid
                 FROM counters"""
        table_data = self._execute_sql(sql)
        hostname = socket.gethostname()
        pid = os.getpid()
        expected = ((key, self.NODE_NAME, value, counter_name, hostname, pid),)
        self.assertEqual(table_data, expected)

    def test_two_store(self):
        """Check that 2 store operations work properly."""
        counter_name = 'test_counter'
        key1 = 'test_counter:dyn'
        value1 = 10
        key2 = 'test_counter:r60'
        value2 = 20
        self.storage.store(key1, value1, counter_name)
        self.storage.store(key2, value2, counter_name)

        sql = """SELECT counter_name, node_name, value, mby, hostname, pid
                 FROM counters"""
        table_data = self._execute_sql(sql)
        hostname = socket.gethostname()
        pid = os.getpid()
        expected = ((key1, self.NODE_NAME, value1, counter_name, hostname, pid),
                    (key2, self.NODE_NAME, value2, counter_name, hostname, pid))
        self.assertEqual(table_data, expected)


if __name__ == '__main__':
    unittest.main()
