"""Unit tests for pool manager module.

:Status: $Id: //prod/main/_is/shared/python/db/test/test_pool_manager.py#14 $
:Authors: ohmelevs
"""

import os
import tempfile
import threading
import time
import unittest2

from shared.db import errors
from shared.db.dbcp import pool_manager
from shared.db.test import mocks


test_conf = """
[test]
db_type=mysql
pool_size=eval(10)
timeout=eval(3)
idle_timeout=eval(1)
log_queries=eval(False)
db=test_db
rw.host=some_host
rw.user=some_user
rw.passwd=some_passwd

[test1]
db_type=mysql
pool_size=eval(10)
timeout=eval(3)
log_queries=eval(False)
db=test_db1
ro.host=some_host
ro.user=some_user
ro.passwd=some_passwd
"""

def _create_conf_file():
        tmp, conf_path = tempfile.mkstemp()
        os.close(tmp)
        with open(conf_path, 'w') as f:
            f.write(test_conf)

        return conf_path


def _clean_up_conf(conf_path):
    os.remove(conf_path)


class TestPoolManager(unittest2.TestCase):
    """PoolManagerImpl test case class."""

    def setUp(self):
        # Make sure we don't leak threads.
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Extra threads left over from a previous test! Will not run.')

        self._conf_path = _create_conf_file()
        self.addCleanup(_clean_up_conf, self._conf_path)
        self._orig_log = pool_manager.logging.getLogger
        pool_manager.logging.getLogger = lambda x: mocks.LogMock()
        self._orig_fact = pool_manager._get_factory
        pool_manager._get_factory = mocks._get_factory
        self._orig_pool = pool_manager.ConnectionPool
        pool_manager.ConnectionPool = mocks.ConnectionPoolMock
        self.pm = pool_manager.DBPoolManagerImpl(self._conf_path)

    def tearDown(self):
        self.pm.shutdown()
        pool_manager._get_factory = self._orig_fact
        pool_manager.logging.getLogger = self._orig_log
        pool_manager.ConnectionPool = self._orig_pool

        # Make sure we don't leak threads.
        self.doCleanups()
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Unit test leaks thread!')

    def test_get_pool1(self):
        self.assertEqual(self.pm.get_ro_pool('test1').conn_factory.pool_name,
                         'test1')
        self.assertEqual(self.pm.get_rw_pool('test').conn_factory.pool_name,
                         'test')

    def test_get_pool2(self):
        self.assertRaises(errors.DBConfigurationError, self.pm.get_ro_pool,
                          'test')
        self.assertRaises(errors.DBConfigurationError, self.pm.get_rw_pool,
                          'test1')
        self.assertRaises(errors.DBConfigurationError, self.pm.get_ro_pool,
                          'not_existed')
        self.assertRaises(errors.DBConfigurationError, self.pm.get_rw_pool,
                          'not_existed')

    def test_clear_pool_cache1(self):
        """Verify clearing pool cache forces new ConnectionPool instances."""
        test_rw_pool = self.pm.get_rw_pool('test')
        test_ro_pool = self.pm.get_ro_pool('test1')

        self.assertIs(self.pm.get_rw_pool('test'), test_rw_pool)
        self.assertIs(self.pm.get_ro_pool('test1'), test_ro_pool)

        self.pm.clear_pool_cache()

        # Recreated.
        self.assertIsNot(self.pm.get_rw_pool('test'), test_rw_pool)
        self.assertIsNot(self.pm.get_ro_pool('test1'), test_ro_pool)

    def test_clear_pool_cache2(self):
        """Verify clearing pool cache removes stored pools."""
        ro = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'new1', False),
                                      1, 1)
        self.pm.set_ro_pool('new1', ro)
        rw = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'new1', True),
                                      1, 1)
        self.pm.set_rw_pool('new2', rw)

        self.assertEqual(self.pm.get_ro_pool('new1'), ro)
        self.assertEqual(self.pm.get_rw_pool('new2'), rw)

        self.pm.clear_pool_cache()

        with self.assertRaises(errors.DBConfigurationError):
            self.pm.get_ro_pool('new1')

        with self.assertRaises(errors.DBConfigurationError):
            self.pm.get_rw_pool('new2')

    def test_validate_typo(self):
        """validate_pools() doesn't like being passed a string by mistake."""
        if __debug__:
            self.assertRaises(AssertionError, self.pm.validate_pools,
                'string not tuple', ())

    def test_validate1(self):
        self.assertEqual(self.pm.validate_pools(('test',), ('test1',)), None)
        self.assertEqual(self.pm.validate_pools(('test',), ()), None)
        self.assertEqual(self.pm.validate_pools((), ('test1',)), None)

    def test_validate2(self):
        self.assertRaises(errors.DBConfigurationError, self.pm.validate_pools,
                          ('not valid',), ())
        self.pm.clear_pool_cache()
        self.assertRaises(errors.DBConfigurationError, self.pm.validate_pools,
                          ('test', 'test1'), ())

    def test_validate3(self):
        self.assertEqual(self.pm.validate_pools(('test',), ('test1',)), None)
        self.pm.get_ro_pool('test1').conn_factory.is_valid = \
                                           lambda conn, timeout=None: False
        self.assertRaises(errors.DBConnectionError, self.pm.validate_pools,
                          ('test',), ('test1',))

    def test_set_pool1(self):
        ro = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'new1', False),
                                      1, 1)
        self.pm.set_ro_pool('new1', ro)
        rw = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'new1', True),
                                      1, 1)
        self.pm.set_rw_pool('new2', rw)
        self.assertEqual(self.pm.validate_pools(('new2',), ('new1',)), None)
        ro = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'new1', False),
                                      1, 1)
        self.pm.set_ro_pool('new2', ro)
        rw = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'new1', True),
                                      1, 1)
        self.pm.set_rw_pool('new2', rw)
        self.assertEqual(self.pm.validate_pools(
            ('new2', 'test'), ('new2', 'new1', 'test1')), None)

    def test_set_pool2(self):
        ro = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'test1', False),
                                      1, 1)
        self.pm.get_ro_pool('test1').conn_factory.is_valid = \
                                           lambda conn, timeout = None: False
        self.assertRaises(errors.DBConnectionError, self.pm.validate_pools,
                          (), ('test1',))
        self.pm.set_ro_pool('test1', ro)
        self.assertEqual(self.pm.validate_pools((), ('test1',)), None)

    def test_set_pool3(self):
        ro = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'test1', False),
                                      1, 1)
        ro.conn_factory.is_valid = lambda conn, timeout = None: False
        self.assertRaises(errors.DBConfigurationError, self.pm.set_ro_pool,
                          'test1', ro)

    def test_set_get_roundtrip_new(self):
        """Verifies pools can be set (no existing pool)."""
        ro = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'test1', False),
                                      1, 1)
        rw = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'test1', True),
                                      1, 1)

        self.pm.set_ro_pool('new', ro)
        self.pm.set_rw_pool('new', rw)

        self.assertIs(self.pm.get_ro_pool('new'), ro)
        self.assertIs(self.pm.get_rw_pool('new'), rw)

    def test_set_get_roundtrip_overwrite(self):
        """Verifies set pools can overwrite existing pools."""

        ro = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'test1', False),
                                      1, 1)
        rw = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'test1', True),
                                      1, 1)

        self.assertIsNot(self.pm.get_ro_pool('test1'), ro)
        self.assertIsNot(self.pm.get_rw_pool('test'), rw)

        self.pm.set_ro_pool('new', ro)
        self.pm.set_rw_pool('new', rw)

        self.assertIs(self.pm.get_ro_pool('new'), ro)
        self.assertIs(self.pm.get_rw_pool('new'), rw)

    def test_idle_handling1(self):
        self.pm.get_rw_pool('test')
        self.assertTrue(self.pm._idle_handler._run)
        self.pm._idle_handler.stop()
        self.assertFalse(self.pm._idle_handler._run)
        self.pm._idle_handler.start()
        self.assertTrue(self.pm._idle_handler._run)

    def test_idle_handling2(self):
        self.pm.get_rw_pool('test')
        self.assertTrue(self.pm._idle_handler._run)
        self.pm.get_ro_pool('test1').conn_factory.is_valid = \
                                           lambda conn, timeout = None: False
        self.assertRaises(errors.DBConnectionError, self.pm.validate_pools, (),
                          ('test1',))
        self.assertTrue(self.pm._idle_handler._run)

    def test_idle_handling3(self):
        self.pm.get_rw_pool('test')
        self.assertTrue(self.pm._idle_handler._run)
        self.pm.clear_pool_cache()
        self.assertFalse(self.pm._idle_handler._run)

    def test_idle_handling4(self):
        self.pm.get_rw_pool('test')
        self.assertTrue(self.pm._idle_handler._run)
        self.pm.clear_pool_cache()
        self.assertFalse(self.pm._idle_handler._run)
        ro = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'test1', False),
                                      1, 1)
        ro.conn_factory.is_valid = lambda conn, timeout = None: False
        self.assertRaises(errors.DBConfigurationError, self.pm.set_ro_pool,
                          'test1', ro)
        self.assertFalse(self.pm._idle_handler._run)

    def test_idle_handling5(self):
        self.pm.get_rw_pool('test')
        self.assertTrue(self.pm._idle_handler._run)
        self.pm.clear_pool_cache()
        self.assertFalse(self.pm._idle_handler._run)
        ro = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'test1', False),
                                      1, 1)
        self.pm.set_ro_pool('test1', ro)
        self.assertTrue(self.pm._idle_handler._run)

    def test_idle_handling6(self):
        self.pm.clear_pool_cache()
        ro = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'test1', False),
                                      1, 1)
        ro.close_idle_connections = self._raise
        self.pm.set_ro_pool('test1', ro)
        self.assertTrue(self.pm._idle_handler._run)

        # This happens in a different thread, so it may not happen right
        # away. There's probably a more robust way of waiting here...
        time.sleep(0.01)
        self.assertTrue(self.pm._idle_handler._log.is_logged(str(ro)))

    def test_get_pools(self):
        ro = mocks.ConnectionPoolMock(10,
                                      mocks.FactoryMock(None, 'new1', False),
                                      1, 1)
        self.pm.set_ro_pool('new1', ro)
        self.assertTrue(ro in self.pm._get_pools())

    def _raise(self, *args, **kwargs):
        raise Exception


class TestNoConfig(unittest2.TestCase):
    """Tests that the PoolManager operates properly when no config file exists.

    This is needed in unit testing or whenever we might explicit create DB
    pools before accessing them.
    """

    def setUp(self):
        pass

    def test_basic(self):
        """We can instantiate and use a DBPoolManager without config."""
        ro_pool = mocks.ConnectionPoolMock(
            10, mocks.FactoryMock(None, 'foo', False), 60, 60)
        rw_pool = mocks.ConnectionPoolMock(
            10, mocks.FactoryMock(None, 'foo2', True), 60, 60)

        my_pm = pool_manager.DBPoolManagerImpl(None)
        try:
            my_pm.set_ro_pool('my_pool', ro_pool)
            my_pm.set_rw_pool('my_pool', rw_pool)
            self.assertIs(my_pm.get_ro_pool('my_pool'), ro_pool)
            self.assertIs(my_pm.get_rw_pool('my_pool'), rw_pool)
        finally:
            my_pm.shutdown()


class TestRaces(unittest2.TestCase):

    def setUp(self):
        # Make sure we don't leak threads.
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Extra threads left over from a previous test! Will not run.')

        self._conf_path = _create_conf_file()
        self.addCleanup(_clean_up_conf, self._conf_path)

    def tearDown(self):
        # Make sure we don't leak threads.
        self.doCleanups()
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Unit test leaks thread!')

    def test_get_pool(self):
        """Only one pool is created with concurrent calls to get_rw_pool()."""
        # We're wrapping mocks.ConnectionPool with a system for counting how
        # many were created and some sleeping.
        conns = list()
        def pool_init(*args, **kwargs):
            factory = mocks.ConnectionPoolMock(*args, **kwargs)
            conns.append(factory)
            time.sleep(0.01)
            return factory

        self.addCleanup(setattr, pool_manager, 'ConnectionPool',
                        pool_manager.ConnectionPool)
        pool_manager.ConnectionPool = pool_init

        self.pm = pool_manager.DBPoolManagerImpl(self._conf_path)
        self.addCleanup(self.pm.shutdown)

        def _target():
            time.sleep(0.001)
            return self.pm.get_rw_pool('test')

        threads = list()
        for i in xrange(10):
            threads.append(threading.Thread(target=_target,
                                             name='get_pool_test-%d' % (i,)))
        for thr in threads:
            thr.start()
        for thr in threads:
            thr.join()

        # Only one ConnectionPool was created, and it's the one we expect.
        self.assertItemsEqual(conns, [self.pm.get_rw_pool('test')])


if __name__ == '__main__':
    unittest2.main()
