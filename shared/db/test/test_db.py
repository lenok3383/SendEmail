"""Tests for top level database functions.

The goal here is to test that access via the shared.db namespace works.

:Status: $Id: //prod/main/_is/shared/python/db/test/test_db.py#2 $
:Author: duncan
"""
import os
import shutil
import tempfile
import threading
import unittest2

import shared.conf
import shared.db
import shared.db.dbcp.pool
import shared.db.errors
import shared.db.test.mocks
import shared.db.utils

test_conf = {
    'test.db_type': 'mysql',
    'test.pool_size': 10,
    'test.timeout': 3,
    'test.idle_timeout': 1,
    'test.log_queries': False,
    'test.db': 'test_db',
    'test.rw.host': 'some_host',
    'test.rw.user': 'some_user',
    'test.rw.passwd': 'some_passwd'}

class TestDB(unittest2.TestCase):

    def setUp(self):
        # Make sure we don't leak threads.
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Extra threads left over from a previous test! Will not run.')

        self.addCleanup(shared.conf.config.clear_config_cache)
        shared.conf.config.set_config(
            'db', shared.conf.config.ConfigFromDict(test_conf))

    def tearDown(self):
        shared.db.shutdown()

        # Make sure we don't leak threads.
        self.doCleanups()
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Unit test leaks thread!')

    def test_pools(self):
        """Verify that the pool access functions work as shared.db.*."""

        pool = shared.db.get_rw_pool('test')
        self.assertTrue(
            isinstance(pool, shared.db.dbcp.pool.ConnectionPool),
            '%s is not a ConnectionPool' % (pool,))

        my_ro_pool = shared.db.test.mocks.ConnectionPoolMock(
            10, shared.db.test.mocks.FactoryMock(None, 'new1', False), 1, 1)
        my_rw_pool = shared.db.test.mocks.ConnectionPoolMock(
            10, shared.db.test.mocks.FactoryMock(None, 'new1', False), 1, 1)

        with self.assertRaises(shared.db.errors.DBConfigurationError):
            shared.db.get_ro_pool('foo')
        shared.db.set_ro_pool('foo', my_ro_pool)
        self.assertIs(shared.db.get_ro_pool('foo'), my_ro_pool)

        with self.assertRaises(shared.db.errors.DBConfigurationError):
            shared.db.get_rw_pool('foo')
        shared.db.set_rw_pool('foo', my_rw_pool)
        self.assertIs(shared.db.get_rw_pool('foo'), my_rw_pool)

        shared.db.validate_pools(('foo',), ('foo',))

        shared.db.clear_pool_cache()
        with self.assertRaises(shared.db.errors.DBConfigurationError):
            shared.db.get_rw_pool('foo')

    def test_retry(self):
        """Verify shared.db.retry is as expected."""
        self.assertEqual(shared.db.retry, shared.db.utils.retry)

    def test_shutdown(self):
        """Ensure shared.db.shutdown() clears global state."""
        self.assertIsNone(shared.db.dbcp._dbpool_manager_instance)
        with self.assertRaises(shared.db.errors.DBConfigurationError):
            shared.db.get_ro_pool('foo')
        self.assertIsNotNone(shared.db.dbcp._dbpool_manager_instance)
        shared.db.shutdown()
        self.assertIsNone(shared.db.dbcp._dbpool_manager_instance)


if __name__ == '__main__':
    unittest2.main()
