"""SecApps base TestCase class.

This class provides test fixtures, additional assertion methods, and many
helper methods to do things that are common in unit test code.

:Author: duncan
:Status: $Id: //prod/main/_is/shared/python/testing/case.py#3 $
"""
import os
import shutil
import tempfile
import threading

import unittest2 as unittest

import shared.conf
import shared.conf.config
import shared.db.temp_db
import shared.db.test.mocks

class TestCase(unittest.TestCase):

    """A test case with extra fixtures, assertions and helpers.

    This augments the default unittest.TestCase class by adding some useful
    helper methods. It is designed to be subclassed.

    None of the methods provided by this test case will run unless explicitly
    called from the subclass code. Specifically, this will not override setUp
    or tearDown, so you do not need to worry about calling these methods in the
    parent class.

    Note: to avoid potential conflicts between methods added to the base class
    and your subclass, be sure to name test methods "test_*" and private
    methods with a leading underscore.
    """

    # Note: When adding methods to this class, it is important to not break
    # existing code.  As a result, it's important to use name mangling (double
    # leading underscores) for private methods, and follow naming conventions
    # for new methods.
    #
    # Currently the following categories and naming conventions are defined:
    # assert*: Additional assertion methods.
    # do_*: Common code snippets, for example to add certain new assertions.
    # get_*: Provide a test resource of some sort.

    def do_thread_check(self):
        """Makes sure that the unit test is not leaking threads.

        When called, verifies that only the main thread exists (otherwise,
        skips the test), and adds a cleanup method to verify that when the test
        is done, all threads that may have been started have stopped.
        """
        # If threads have already been started, we can't run this test fairly,
        # so we skip it.
        self.__thread_check('Extra threads were started before this test.',
                            skip=True)

        # Afterwards, if there are threads running, we caused it. That's bad.
        self.addCleanup(self.__thread_check,
                        'This test started threads that are still alive.')

    def __thread_check(self, msg, skip=False):
        if threading.enumerate() != [threading.current_thread()]:
            if skip:
                self.skipTest(msg)
            else:
                # Fail. Try again as an assertion so we get better reporting.
                self.assertEqual(threading.enumerate(),
                                 [threading.current_thread()], msg)

    def get_temp_dir(self):
        """Creates a temporary directory.

        It will be deleted at cleanup time.
        """
        temp_dir = tempfile.mkdtemp(prefix='unittest_%s_' % (self.id()))
        self.addCleanup(shutil.rmtree, temp_dir)
        return temp_dir

    _UNITTEST_DB_CONF = '~/.unittest-db.conf'

    def get_temp_database_manager(self):
        """Instantiates a temporary database manager.

        Creates a temporary database manager
        (`shared.db.temp_db.TemporaryDBManager`) using the configuration file
        in ``~/.unittest-db.conf``. Adds cleanup hooks to shutdown dbcp when
        the test is done.
        """
        self.addCleanup(shared.db.shutdown)

        config_file = os.path.expanduser(self._UNITTEST_DB_CONF)
        if not os.path.exists(config_file):
            self.skipTest('Could not find unittest DB configuration in %s' %
                          (self._UNITTEST_DB_CONF,))
            return

        config = shared.conf.config.Config(config_file)

        db_manager = shared.db.temp_db.TemporaryDBManager(config, 'unittest')
        self.addCleanup(db_manager.cleanup)

        return db_manager

    def get_mock_database_pool(self, max_size=None, timeout=None, idle=None,
                               config=None, pool_name=None, read_write=None):
        """Instantiates a mock database pool."""
        self.addCleanup(shared.db.clear_pool_cache)

        return shared.db.test.mocks.ConnectionPoolMock(
            max_size,
            shared.db.test.mocks.FactoryMock(config, pool_name, read_write),
            timeout, idle)
