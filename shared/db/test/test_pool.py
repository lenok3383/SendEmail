"""Unit tests for connection pool module.

:Status: $Id: //prod/main/_is/shared/python/db/test/test_pool.py#13 $
:Authors: ohmelevs
"""
import os
import threading
import time
import unittest2

from shared.db import errors
from shared.db.dbcp import pool
from shared.db.test import mocks


# We don't care about query and result of the execution.
q_res = 'query'


class TestPool(unittest2.TestCase):
    """ConnectionPool test case class."""

    def setUp(self):
        # Make sure we don't leak threads.
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Extra threads left over from a previous test! Will not run.')

        self._orig_log = pool.logging.getLogger
        pool.logging.getLogger = lambda x: mocks.LogMock()
        self.factory = mocks.FactoryMock(None, 'test')

        app_sql_tag = os.environ.get('APP_SQL_TAG')
        if app_sql_tag is not None:
            self.addCleanup(os.environ.__setitem__, 'APP_SQL_TAG', app_sql_tag)
            del os.environ['APP_SQL_TAG']

        self._threads = list()

    def tearDown(self):
        pool.logging.getLogger = self._orig_log

        for thr in self._threads:
            thr.join(10)
            if thr.is_alive():
                self.fail('Thread will not join: %s' % (thr,))

        # Make sure we don't leak threads.
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Unit test leaks thread!')

    def test_connection1(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        with cp.connection() as conn:
            self.assertTrue(conn.cursor().execute(q_res), q_res)
            self.assertEqual(cp.num_idle(), 0)
        self.assertEqual(cp.num_idle(), 1)

    def test_connection2(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        try:
            with cp.connection() as conn:
                self.assertEqual(cp.num_active(), 1)
                raise Exception
            self.assertEqual(cp.num_idle(), 1)
        except AssertionError:
            raise
        except:
            pass

    def test_transaction1(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        with cp.transaction() as cursor:
            cursor.execute(q_res)
        self.assertTrue(q_res in cursor.executed_queries)
        self.assertTrue('BEGIN' in cursor.executed_queries and
                        'COMMIT' in cursor.executed_queries)

    def test_transaction2(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        try:
            with cp.transaction() as cursor:
                raise Exception
        except:
            pass
        self.assertTrue('BEGIN' in cursor.executed_queries and
                        'COMMIT' not in cursor.executed_queries and
                        'ROLLBACK' in cursor.executed_queries)

    def test_transaction3(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        with cp.transaction() as cursor:
            cursor.execute(q_res)
        self.assertEqual(len(cursor.executed_queries), 3)

    def test_transaction4(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        with cp.transaction() as cursor:
            cursor.executemany('query %s', (1, 2, 3))
        self.assertEqual(len(cursor.executed_queries), 5)

    def test_num_idle1(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        self.assertTrue(self._execute(cp), q_res)
        self.assertEqual(cp.num_idle(), 1)

    def test_num_idle2(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        for i in xrange(10):
            self.assertTrue(self._execute(cp), q_res)
        self.assertEqual(cp.num_idle(), 1)

    def test_num_idle3(self):
        cp = pool.ConnectionPool(5, self.factory, 1, 100)
        for i in xrange(5):
            self._execute_apart(cp, 0.3)
        self.assertEqual(cp.num_active(), 5)
        self.assertFalse(cp.is_available())
        time.sleep(0.7)
        self.assertTrue(cp.is_available())
        self.assertEqual(cp.num_active(), 0)
        self.assertEqual(cp.num_idle(), 5)

    def test_is_available1(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        self.assertTrue(cp.is_available())
        for i in xrange(10):
            self.assertTrue(self._execute(cp), q_res)
        self.assertTrue(cp.is_available())

    def test_is_available2(self):
        cp = pool.ConnectionPool(1, self.factory, 1, 1)
        self.assertTrue(cp.is_available())
        with cp.connection() as conn:
            self.assertTrue(conn.cursor().execute(q_res), q_res)
            self.assertFalse(cp.is_available())
        self.assertTrue(cp.is_available())

    def test_is_available3(self):
        cp = pool.ConnectionPool(2, self.factory, 1, 1)
        self.assertTrue(cp.is_available())
        with cp.connection() as conn:
            self.assertTrue(conn.cursor().execute(q_res), q_res)
            self.assertTrue(cp.is_available())
            with cp.connection() as conn2:
                self.assertTrue(conn2.cursor().execute(q_res), q_res)
                self.assertFalse(cp.is_available())
            self.assertTrue(cp.is_available())
        self.assertTrue(cp.is_available())

    def test_num_active1(self):
        cp = pool.ConnectionPool(1, self.factory, 1, 1)
        self.assertEqual(cp.num_active(), 0)
        with cp.connection() as conn:
            self.assertEqual(cp.num_active(), 1)
            self.assertTrue(conn.cursor().execute(q_res), q_res)
        self.assertEqual(cp.num_active(), 0)

    def test_num_active2(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        self.assertEqual(cp.num_active(), 0)
        with cp.connection() as conn:
            self.assertEqual(cp.num_active(), 1)
            self.assertTrue(conn.cursor().execute(q_res), q_res)
            with cp.connection() as conn2:
                self.assertEqual(cp.num_active(), 2)
                self.assertTrue(conn2.cursor().execute(q_res), q_res)
        self.assertEqual(cp.num_active(), 0)

    def test_is_in_use(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        with cp.connection() as conn:
            self.assertTrue(conn.cursor().execute(q_res), q_res)
            self.assertTrue(cp.is_in_use(conn))
        self.assertFalse(cp.is_in_use(conn))

    def test_timeout1(self):
        cp = pool.ConnectionPool(1, self.factory, 1, 1)
        with cp.connection() as conn:
            self.assertTrue(conn.cursor().execute(q_res), q_res)
            self.assertRaises(errors.TimeoutError, self._execute, cp, 0)

    def test_timeout2(self):
        cp = pool.ConnectionPool(2, self.factory, 1, 1)
        with cp.connection() as conn:
            self.assertTrue(conn.cursor().execute(q_res), q_res)
            with cp.connection() as conn1:
                self.assertEqual(conn1.cursor().execute(q_res), q_res)

    def test_timeout3(self):
        cp = pool.ConnectionPool(2, self.factory, 2, 1)
        self._execute_apart(cp, 0.2)
        self.assertTrue(self._execute(cp), q_res)

    def test_timeout4(self):
        cp = pool.ConnectionPool(1, self.factory, 1, 1)
        self._execute_apart(cp, 1.2)
        self.assertRaises(errors.TimeoutError, self._execute, cp)

    def test_timeout5(self):
        cp = pool.ConnectionPool(1, self.factory, 100, 1)
        self._execute_apart(cp, 1.2)
        self.assertRaises(errors.TimeoutError, self._execute, cp, 0, 1)

    def test_shutdown1(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 100)
        self.assertTrue(self._execute(cp), q_res)
        cp.shutdown()
        self.assertEqual(cp.num_active(), 0)
        self.assertEqual(cp.num_idle(), 0)

    def test_shutdown2(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 100)
        cp.shutdown()
        self.assertEqual(cp.num_active(), 0)
        self.assertEqual(cp.num_idle(), 0)

    def test_shutdown3(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 100)
        self._execute_apart(cp, 0.5)
        self.assertEqual(cp.num_active(), 1)
        cp.shutdown()
        self.assertEqual(cp.num_active(), 0)
        self.assertEqual(cp.num_idle(), 0)

    def test_close_idle1(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 0.1)
        self._execute(cp)
        self.assertEqual(cp.num_idle(), 1)
        time.sleep(0.2)
        cp.close_idle_connections()
        self.assertEqual(cp.num_idle(), 0)

    def test_close_idle2(self):
        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        self._execute(cp)
        self.assertEqual(cp.num_idle(), 1)
        cp.close_idle_connections()
        self.assertEqual(cp.num_idle(), 1)

    def test_app_sql_tag(self):
        """Using the APP_SQL_TAG env var leads to commented queries."""
        tag = 'Hi mom!'

        # At this point APP_SQL_TAG is guaranteed to not exist (see setUp), we
        # need to restore it to this state.
        os.environ['APP_SQL_TAG'] = tag
        self.addCleanup(os.environ.__delitem__, 'APP_SQL_TAG')

        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        with cp.transaction() as cursor:
            cursor.execute(q_res)
        self.assertEqual(len(cursor.executed_queries), 3)
        for query in cursor.executed_queries:
            self.assertTrue(
                query.endswith('-- ' + tag),
                '"%s" does not end with app_sql_tag %s' % (query, tag))

    def test_app_sql_tag_trailing_semicolon(self):
        """APP_SQL_TAG not appended after trailing semi-colon."""
        tag = 'Hi mom!'
        # At this point APP_SQL_TAG is guaranteed to not exist (see setUp), we
        # need to restore it to this state.
        os.environ['APP_SQL_TAG'] = tag
        self.addCleanup(os.environ.__delitem__, 'APP_SQL_TAG')

        cp = pool.ConnectionPool(10, self.factory, 1, 1)
        semicolon_query = q_res + ';'
        with cp.transaction() as cursor:
            cursor.execute(semicolon_query)
        self.assertEqual(len(cursor.executed_queries), 3)

        self.assertEqual(
            cursor.executed_queries[1], semicolon_query,
            '%s improperly modified before executing' % (semicolon_query,))

    def _execute(self, pool, sleep=0, timeout=None, started_callback=None):
        with pool.connection(timeout) as conn:
            if started_callback:
                started_callback()
            time.sleep(sleep)
            return conn.cursor().execute(q_res)

    def _execute_apart(self, pool, sleep=0, timeout=None):
        started = []
        thr = threading.Thread(
            target=self._execute,
            args=(pool, sleep, timeout, lambda: started.append(True)),
            name='TestPool._execute_apart')
        thr.start()
        for i in xrange(1000):
            if started:
                break
            time.sleep(0.001)

        self._threads.append(thr)


if __name__ == '__main__':
    unittest2.main()

