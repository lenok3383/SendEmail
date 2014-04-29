"""Test unit test fixtures.

:Author: duncan
:Status: $Id: //prod/main/_is/shared/python/testing/test/test_case.py#3 $
"""

import os
import threading
import time
import unittest2

import shared.db.temp_db
import shared.testing.case

class TestTestCase(unittest2.TestCase):

    def _assert_test_errors(self, test_case, regexp):
        result = unittest2.TestResult()
        test_case.run(result)
        self.assertFalse(result.wasSuccessful(), result)
        self.assertEqual(len(result.errors), 1)
        self.assertRegexpMatches(result.errors[0][1], regexp)

    def _assert_test_skips(self, test_case, regexp):
        result = unittest2.TestResult()
        test_case.run(result)
        self.assertTrue(result.wasSuccessful(), result)
        self.assertEqual(len(result.skipped), 1)
        self.assertRegexpMatches(result.skipped[0][1], regexp)

    def _assert_test_passes(self, test_case):
        result = unittest2.TestResult()
        test_case.run(result)
        self.assertTrue(result.wasSuccessful(), result.errors)
        self.assertEqual(result.testsRun, 1)

    def test_thread_check_test_create_threads(self):
        """add_thread_check() detects thread creation during test."""

        class ThreadCase(shared.testing.case.TestCase):
            def setUp(self):
                self.do_thread_check()

            def runTest(self):
                self.thr = threading.Thread(target=lambda: time.sleep(0.1))
                self.thr.start()

        thread_case = ThreadCase()
        self._assert_test_errors(thread_case,
                                 'started threads that are still alive')
        thread_case.thr.join()

    def test_thread_check_thread_started_before(self):
        """add_thread_check() skips if thread exists before test."""
        thr = threading.Thread(target=lambda: time.sleep(0.1))
        thr.start()
        self.addCleanup(thr.join)

        class ThreadCase(shared.testing.case.TestCase):
            def setUp(self):
                self.do_thread_check()

            def runTest(self):
                pass

        self._assert_test_skips(ThreadCase(),
                                'threads were started before this test')

    def test_get_temp_dir(self):
        """get_temp_dir() creates and deletes temp dir with suitable name."""

        class TempDir(shared.testing.case.TestCase):
            def setUp(self):
                self._tmp = self.get_temp_dir()

            def runTest(self):
                self.assertTrue(os.path.exists(self._tmp))
                with open(os.path.join(self._tmp, 'some_file'), 'w') as f:
                    # We can write to it.
                    f.write('hi')

        test_case = TempDir()
        self._assert_test_passes(test_case)
        self.assertFalse(os.path.exists(test_case._tmp),
                         '%s still exists' % (test_case._tmp,))
        self.assertEqual(os.path.basename(test_case._tmp)[:9],
                         'unittest_')
        self.assertRegexpMatches(
            os.path.basename(test_case._tmp), 'TempDir',
            'test name is not in the dir name')

    def test_get_temp_db_manager_no_config(self):
        """get_temp_db_manager() skips if no config file found."""
        self.addCleanup(setattr, shared.testing.case.TestCase,
                        '_UNITTEST_DB_CONF',
                        shared.testing.case.TestCase._UNITTEST_DB_CONF)
        shared.testing.case.TestCase._UNITTEST_DB_CONF = \
            '/unittest-db/does-not-exist'

        class TempDBNoConfig(shared.testing.case.TestCase):
            def setUp(self):
                self._db_manager = self.get_temp_database_manager()

            def runTest(self):
                pass

        self._assert_test_skips(TempDBNoConfig(),
                                'Could not find unittest DB configuration')

    def test_get_temp_db_manager_with_config(self):
        """get_temp_db_manager() returns a temp DB manager."""

        if not os.path.exists(os.path.expanduser(
                shared.testing.case.TestCase._UNITTEST_DB_CONF)):
            # We could probably mock things to do this test even without a
            # ~/.unittest-db.conf.
            self.skipTest('Need ~/.unittest-db.conf for this test.')

        class TempDBWithConfig(shared.testing.case.TestCase):
            def setUp(self):
                self._db_manager = self.get_temp_database_manager()

            def runTest(self):
                self.assertTrue(
                    isinstance(self._db_manager,
                               shared.db.temp_db.TemporaryDBManager))

        self._assert_test_passes(TempDBWithConfig())
