"""Unit tests for db.utils module.

:Status: $Id: //prod/main/_is/shared/python/db/test/test_utils.py#11 $
:Authors: ohmelevs
"""

import MySQLdb
import threading
import unittest2

from shared.db import errors
from shared.db import utils
from shared.db.test import mocks

class TestRetry(unittest2.TestCase):
    """Retry decorator test case class."""

    def setUp(self):
        # Make sure we don't leak threads.
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Extra threads left over from a previous test! Will not run.')

        self._count_retry = 0
        self._orig_sleep = utils.time.sleep
        utils.time.sleep = self._sleep_mock
        self._orig_log = utils.logging.getLogger
        utils.logging.getLogger = lambda x: mocks.LogMock()

    def tearDown(self):
        utils.time.sleep = self._orig_sleep
        utils.logging.getLogger = self._orig_log

        # Make sure we don't leak threads.
        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Unit test leaks thread!')

    def test_successful_execution1(self):
        func = utils.retry()(self._test_func)
        self.assertEqual(func('test'), 'test')
        self.assertEqual(func(), None)
        self.assertEqual(func(1), 1)

    def test_successful_execution2(self):
        func = utils.retry(0, 0)(self._test_func)
        self.assertEqual(func('test'), 'test')
        self.assertEqual(func(), None)
        self.assertEqual(func(1), 1)

    def test_successful_execution3(self):
        func = utils.retry(100, 100)(self._test_func)
        self.assertEqual(func('test'), 'test')
        self.assertEqual(func(), None)
        self.assertEqual(func(1), 1)

    def test_successful_execution4(self):
        func = utils.retry(None, None)(self._test_func)
        self.assertEqual(func('test'), 'test')
        self.assertEqual(func(), None)
        self.assertEqual(func(1), 1)

    def test_noretry_errors1(self):
        func = utils.retry()(self._raise_test_func)
        self.assertRaises(Exception, func, Exception)
        self.assertEqual(self._count_retry, 0)

    def test_noretry_errors2(self):
        func = utils.retry(None, None)(self._raise_test_func)
        self.assertRaises(Exception, func, Exception)
        self.assertEqual(self._count_retry, 0)

    def test_noretry_errors3(self):
        func = utils.retry(0, 0)(self._raise_test_func)
        self.assertRaises(Exception, func, Exception)
        self.assertEqual(self._count_retry, 0)

    def test_noretry_errors4(self):
        func = utils.retry(100, 100)(self._raise_test_func)
        self.assertRaises(Exception, func, Exception)
        self.assertEqual(self._count_retry, 0)

    def test_noretry_raise1(self):
        func = utils.retry(0, 0)(self._raise_test_func)
        self.assertRaises(errors.DBConnectionError, func,
                          errors.DBConnectionError)
        self.assertEqual(self._count_retry, 0)
        self.assertRaises(errors.DBRetryError, func,
                          errors.DBRetryError)
        self.assertEqual(self._count_retry, 0)

    def test_noretry_raise2(self):
        func = utils.retry(100, 0)(self._raise_test_func)
        self.assertRaises(errors.DBConnectionError, func,
                          errors.DBConnectionError)
        self.assertEqual(self._count_retry, 0)

    def test_noretry_raise3(self):
        func = utils.retry(0, 100)(self._raise_test_func)
        self.assertRaises(errors.DBRetryError, func,
                          errors.DBRetryError)
        self.assertEqual(self._count_retry, 0)

    def test_retry1(self):
        func = utils.retry(100, 0)(self._raise_test_func)
        self.assertRaises(errors.DBRetryError, func, errors.DBRetryError)
        self.assertEqual(self._count_retry, 100)

    def test_retry2(self):
        func = utils.retry(0, 100)(self._raise_test_func)
        self.assertRaises(errors.DBConnectionError, func,
                          errors.DBConnectionError)
        self.assertEqual(self._count_retry, 100)

    def _test_func(self, x=None):
        return x

    def _raise_test_func(self, exc_class):
        raise exc_class(MySQLdb.OperationalError())

    def _sleep_mock(self, timeout):
        # Just increment sleep counter.
        self._count_retry += 1


if __name__ == '__main__':
    unittest2.main()
