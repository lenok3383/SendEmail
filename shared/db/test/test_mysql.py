"""Unit tests for mysql db module.

:Status: $Id: //prod/main/_is/shared/python/db/test/test_mysql.py#16 $
:Authors: ohmelevs
"""

import threading
import time
import unittest2

from shared.db import errors
from shared.db.dbcp import connection_factory as cf
from shared.db.dbcp import mysql
from shared.db.test import mocks


class TestMySQL(unittest2.TestCase):
    """MySQL module test case class."""

    _fake_conf = {'some.db': 'name', 'some.ro.host': 'host',
                  'some.ro.user':'user', 'some.ro.password':'passwd',
                  'some.connect_timeout':'timeout'}

    def setUp(self):
        # Make sure we don't leak threads.
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Extra threads left over from a previous test! Will not run.')

        self._orig_log = cf.logging.getLogger
        cf.logging.getLogger = lambda x: mocks.LogMock()

        self._orig_env_get = cf.os.environ.get
        cf.os.environ.get = lambda x: 'unit testing'

        self._orig_context = cf.execute_log_context
        cf.execute_log_context = mocks.ContextStub
        self._orig_many_context = cf.executemany_log_context
        cf.executemany_log_context = mocks.ContextStub

        self._orig_conn = mysql.MySQLdb.connect
        mysql.MySQLdb.connect = mocks.connect

        self._orig_cursor = mysql.MySQLdbCursor
        mysql.MySQLdbCursor = mocks.MySQLdbCursorMock

        # some dummy factory.
        self.factory = mysql.MySQLConnectionFactory(self._fake_conf, 'some')

    def tearDown(self):
        cf.execute_log_context = self._orig_context
        cf.executemany_log_context = self._orig_many_context
        cf.os.environ.get = self._orig_env_get
        cf.logging.getLogger = self._orig_log

        mysql.MySQLdbCursor = self._orig_cursor
        mysql.MySQLdb.connect = self._orig_conn

        # Make sure we don't leak threads.
        self.doCleanups()
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Unit test leaks thread!')

    def test_bad_params(self):
        with self.assertRaises(errors.DBConfigurationError):
            bad_factory = mysql.MySQLConnectionFactory(
                self._fake_conf, 'some', read_write=True)

    def test_more_bad_params(self):
        conf = self._fake_conf.copy()
        for key in ('some.db', 'some.ro.host', 'some.ro.user'):
            self._fake_conf = conf
            del conf[key]
            with self.assertRaises(errors.DBConfigurationError):
                bad_factory = mysql.MySQLConnectionFactory(
                    self._fake_conf, 'some', read_write=False)

    def test_cursor(self):
        cur = self.factory.get_conn().cursor()
        self.assertTrue(cur.execute('query'), 'query -- unit testing')
        self.assertTrue(cur.execute('query: %s', 'args'),
                        'query: args -- unit testing')
        self.assertTrue(cur.executemany('query: %s', ('args')),
                        ['query: args -- unit testing'])

    def test_map_exception(self):
        self.assertRaises(Exception, _execute_with,
                          self.factory.error_translation(), '')

        with self.assertRaises(errors.DBConfigurationError) as e:
            with self.factory.error_translation():
                raise mysql.MySQLdb.OperationalError(
                    mysql.WRONG_HOST, 'Wrong host!')
        self.assertEqual(e.exception[0], mysql.WRONG_HOST)

        with self.assertRaises(errors.DBConnectionError) as e:
            with self.factory.error_translation():
                raise mysql.MySQLdb.OperationalError(
                    mysql.CON_COUNT, 'Too many connections')
        self.assertEqual(e.exception[0], mysql.CON_COUNT)

        with self.assertRaises(errors.DBRetryError) as e:
            with self.factory.error_translation():
                raise mysql.MySQLdb.OperationalError(
                    mysql.LOCK_DEADLOCK, 'Deadlock')
        self.assertEqual(e.exception[0], mysql.LOCK_DEADLOCK)

    def test_validation1(self):
        conn = self.factory.get_conn()
        self.assertTrue(self.factory.is_valid(conn, 1))
        conn._is_valid = False
        self.assertFalse(self.factory.is_valid(conn))

    def test_validation2(self):
        conn = self.factory.get_conn()
        conn._sleep_time = 0.3
        start_time = time.time()
        self.factory.is_valid(conn)
        self.assertAlmostEqual(time.time() - start_time, 0.3, 1)
        start_time = time.time()
        self.factory.is_valid(conn, 0.1)
        self.assertAlmostEqual(time.time() - start_time, 0.1, 1)

        # is_valid() creates a thread to check the connection. We don't want to
        # leave active threads in the unit tests as it's a form of global
        # state, so we'll clean them up here.
        for thread in threading.enumerate():
            if thread == threading.current_thread():
                continue
            thread.join()

    def test_in_clause(self):
        in_conv = mysql.conversions[mysql.InClause]
        self.assertEqual(in_conv(mysql.InClause([1, 2, 3]), mysql.conversions),
                         '(1,2,3)')
        self.assertEqual(in_conv(mysql.InClause([1]), mysql.conversions),
                         '(1)')
        self.assertEqual(in_conv(mysql.InClause([1]), mysql.conversions),
                         '(1)')
        self.assertEqual(in_conv(mysql.InClause(['a', 'b', "c"]),
                                 mysql.conversions),
                         '(\'a\',\'b\',\'c\')')

    def test_like_clause(self):
        like_conv = mysql.conversions[mysql.LikeClause]
        self.assertEqual(like_conv(mysql.LikeClause(1),
                                   mysql.conversions), "'1'")
        self.assertEqual(like_conv(mysql.LikeClause('%a_b$'),
                                   mysql.conversions), "'%a_b$'")
        self.assertEqual(like_conv(mysql.LikeClause("'"),
                                   mysql.conversions), "'\\''")
        self.assertEqual(like_conv(mysql.LikeClause('\n'),
                                   mysql.conversions), "'\n'")
        # a\\n - will be evaluated by MySQL LIKE into
        # 'a<unknown unescaped backslash>n', so the result will be 'an' and
        # we actually have nothing to escape
        self.assertEqual(like_conv(mysql.LikeClause('a\\n'),
                                   mysql.conversions), "'a\\n'")
        # if user need 'a<slash>n' he/she have to escape slash twice
        self.assertEqual(like_conv(mysql.LikeClause('a\\\\n'),
                                   mysql.conversions), "'a\\\\\\\\n'")
        self.assertEqual(like_conv(mysql.LikeClause('\_'),
                                   mysql.conversions), "'\\\\_'")
        self.assertEqual(like_conv(mysql.LikeClause('\\_'),
                                   mysql.conversions), "'\\\\_'")
        self.assertEqual(like_conv(mysql.LikeClause('\\\\_'),
                                   mysql.conversions), "'\\\\\\\\_'")
        self.assertEqual(like_conv(mysql.LikeClause('\\%'),
                                   mysql.conversions), "'\\\\%'")
        self.assertEqual(like_conv(mysql.LikeClause('\\\\%'),
                                   mysql.conversions), "'\\\\\\\\%'")


class TestExecutionContext(unittest2.TestCase):
    """Execution context test case class."""

    query = 'Some query'
    args = ['Some args']
    err = 'Some exception'

    def setUp(self):
        self._log = mocks.LogMock()

    def tearDown(self):
        pass

    def test_execute_no_log_1(self):
        dec = cf.execute_log_context(self._log, self.query, None, False)
        _execute_with(dec)
        self.assertEqual(len(self._log.logged), 0)

    def test_execute_no_log_2(self):
        dec = cf.execute_log_context(self._log, self.query, self.args, False)
        _execute_with(dec)
        self.assertEqual(len(self._log.logged), 0)

    def test_execute_err_1(self):
        dec = cf.execute_log_context(self._log, self.query, self.args, False)
        self.assertRaises(Exception, _execute_with, dec, self.err)
        self.assertEqual(len(self._log.logged), 0)
        self.assertFalse(self._log.is_logged(self.query))
        self.assertFalse(self._log.is_logged(self.args[0]))
        self.assertFalse(self._log.is_logged(self.err))

    def test_execute_err_2(self):
        dec = cf.execute_log_context(self._log, self.query, self.args, True)
        self.assertRaises(Exception, _execute_with, dec, self.err)
        self.assertEqual(len(self._log.logged), 1)
        self.assertTrue(self._log.is_logged(self.query))
        self.assertTrue(self._log.is_logged(self.args[0]))

    def test_execute_log(self):
        dec = cf.execute_log_context(self._log, self.query, self.args, True)
        _execute_with(dec)
        self.assertEqual(len(self._log.logged), 1)
        self.assertTrue(self._log.is_logged(self.query))
        self.assertTrue(self._log.is_logged(self.args[0]))

    def test_executemany_no_log_1(self):
        dec = cf.executemany_log_context(self._log, False)
        _execute_with(dec)
        self.assertEqual(len(self._log.logged), 0)

    def test_executemany_no_log_2(self):
        dec = cf.executemany_log_context(self._log, False)
        _execute_with(dec)
        self.assertEqual(len(self._log.logged), 0)

    def test_executemany_err_1(self):
        dec = cf.executemany_log_context(self._log, False)
        self.assertRaises(Exception, _execute_with, dec, self.err)
        self.assertEqual(len(self._log.logged), 0)
        self.assertFalse(self._log.is_logged(self.query))
        self.assertFalse(self._log.is_logged(self.args[0]))
        self.assertFalse(self._log.is_logged(self.err))

    def test_executemany_err_2(self):
        dec = cf.executemany_log_context(self._log, True)
        self.assertRaises(Exception, _execute_with, dec, self.err)
        self.assertEqual(len(self._log.logged), 2)
        self.assertFalse(self._log.is_logged(self.query))
        self.assertFalse(self._log.is_logged(self.args[0]))
        self.assertFalse(self._log.is_logged(self.err))

    def test_executemany_log(self):
        dec = cf.executemany_log_context(self._log, True)
        _execute_with(dec)
        self.assertEqual(len(self._log.logged), 2)
        self.assertFalse(self._log.is_logged(self.query))
        self.assertFalse(self._log.is_logged(self.args[0]))


def _execute_with(dec, err=None, err_class=Exception):
    with dec:
        if err is not None:
            raise err_class(err)


if __name__ == '__main__':
    unittest2.main()
