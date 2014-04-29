"""Unit tests for temporary DB code.

:Author: duncan
"""

import os
import pwd
import random
import shutil
import tempfile
import threading
import unittest2
import warnings

import MySQLdb

import shared.conf
import shared.db
import shared.db.dbcp.pool
import shared.db.errors
import shared.db.temp_db

CREATE_SQL = """
-- Create a new DB!
source schema/foo.sql;
"""

SCHEMA_SQL = """
CREATE TABLE foo (
  a int auto_increment primary key,
  b varchar(255)
) Engine=InnoDB;
"""

INSERT_SQL = """
INSERT INTO foo (b) VALUES ('hello!');
"""

class TestableTempDBManager(shared.db.temp_db.TemporaryDBManager):

    """Subclass temp db mgr to make wait_for_replication() easier to test."""

    def _is_same_host(self, *pools):
        return False


class TestTempDBBase(unittest2.TestCase):

    """Test the temporary database functionality."""

    def setUp(self):
        # Make sure we don't leak threads.
        self.assertItemsEqual(threading.enumerate(),
                              [threading.current_thread()],
                              'Extra threads exist, aborting.')

        unittest_db_conf = os.path.expanduser('~/.unittest-db.conf')
        if not os.path.exists(unittest_db_conf):
            self.skipTest('No ~/.unittest-db.conf found, can not create '
                          'temporary databases.')
        self._conf = shared.conf.config.Config(unittest_db_conf)

        self.addCleanup(shared.db.shutdown)

        # Make sure we don't try to use a db.conf by mistake.
        shared.conf.config.set_config(
            'db', shared.conf.config.ConfigFromDict({}))
        self.addCleanup(shared.conf.config.clear_config_cache)

    def tearDown(self):
        # Make sure we don't leak threads.
        self.doCleanups()
        self.assertItemsEqual(threading.enumerate(),
                              [threading.current_thread()],
                              'Unit tests leaks threads!')

    def _init(self, add_cleanup=True):
        dbm = TestableTempDBManager(self._conf, 'unittest')
        if add_cleanup:
            self.addCleanup(dbm.cleanup)
        return dbm


class TestTempDB(TestTempDBBase):

    def test_init(self):
        """TemporaryDBManager can be initialized and creates a DB pool."""
        self._dbm = self._init()
        self.assertIsInstance(self._dbm,
                              shared.db.temp_db.TemporaryDBManager)

        creator_pool = shared.db.get_rw_pool('temporary_db_creator')
        self.assertIsInstance(creator_pool, shared.db.dbcp.pool.ConnectionPool)

        # Make sure it works.
        with creator_pool.transaction() as c:
            self.assertEqual(c.execute('SELECT VERSION()'), 1)
            self.assertRegexpMatches(c.fetchone()[0], '^\d')

    def test_create_db(self):
        """Tests create_db() creates MySQL databases and DB pools."""
        self._dbm = self._init()
        self._dbm.create_db('my_pool')

        my_rw = shared.db.get_rw_pool('my_pool')
        self.assertIsInstance(my_rw, shared.db.dbcp.pool.ConnectionPool)

        my_ro = shared.db.get_ro_pool('my_pool')
        self.assertIsInstance(my_ro, shared.db.dbcp.pool.ConnectionPool)

        current_user = pwd.getpwuid(os.getuid())[0]
        with my_ro.transaction() as c:
            c.execute('SELECT DATABASE()')
            self.assertRegexpMatches(
                c.fetchone()[0], '^%s_tempdb_[0-9a-f]{8}' % (current_user,))

    def test_load_sql(self):
        """Tests sql_file parameter to create_db() loads SQL into new DB."""
        dir_name = tempfile.mkdtemp(prefix='unittest_test_temp_db_')
        self.addCleanup(shutil.rmtree, dir_name)

        os.mkdir(os.path.join(dir_name, 'schema'))

        for fname, data in (('CREATE_DB.sql', CREATE_SQL),
                            ('schema/foo.sql', SCHEMA_SQL),
                            ('INSERT_DATA.sql', INSERT_SQL)):

            path = os.path.join(dir_name, fname)
            with open(path, 'w') as f:
                f.write(data)

        sql_files = [os.path.join(dir_name, fname)
                     for fname in ('CREATE_DB.sql', 'INSERT_DATA.sql')]

        self._dbm = self._init()
        self._dbm.create_db('my_pool', sql_files=sql_files)

        my_rw = shared.db.get_ro_pool('my_pool')
        with my_rw.transaction() as c:
            self.assertEqual(c.execute('SELECT * FROM foo'), 1)
            self.assertEqual(c.fetchall(), ((1, 'hello!'),))

    def _assert_db_pool_is_db_name(self, pool, db_name):
        with pool.transaction() as c:
            c.execute('SELECT DATABASE()')
            self.assertEqual(c.fetchone()[0], db_name)

    def test_db_name(self):
        """Tests db_name parameter to create_db() works."""
        self._dbm = self._init()
        db_name = '%s_test_tempdb_%08x' % (pwd.getpwuid(os.getuid())[0],
                                           random.getrandbits(32),)
        self._dbm.create_db('my_pool', db_name=db_name)
        my_ro = shared.db.get_ro_pool('my_pool')
        self._assert_db_pool_is_db_name(my_ro, db_name)

    def _assert_db_does_not_exist(self, db_name):
        creator_pool = shared.db.get_rw_pool('temporary_db_creator')
        with self.assertRaisesRegexp(MySQLdb.OperationalError,
                                     'database doesn\'t exist'):
            with creator_pool.transaction() as c:
                c.execute('DROP DATABASE %s' % (db_name,))

    def test_drop_db_implicit(self):
        """Tests database manager drops databases when it goes out of scope."""
        db_name_1 = '%s_test_tempdb_%08x' % (pwd.getpwuid(os.getuid())[0],
                                             random.getrandbits(32),)
        db_name_2 = db_name_1 + '_2'

        def _func():
            dbm = self._init(add_cleanup=False)
            dbm.create_db('my_pool_1', db_name=db_name_1)
            dbm.create_db('my_pool_2', db_name=db_name_2)

        # dbm is local to _func, so it will be deleted (with CPython at least)
        # when it goes out of scope.  This is supposed to run cleanup().
        with warnings.catch_warnings(record=True) as emitted_warnings:
            _func()
        self.assertEqual(len(emitted_warnings), 1)
        self.assertRegexpMatches(str(emitted_warnings[0].message),
                         '__del__ called')
        self.assertEqual(emitted_warnings[0].category,
                         shared.db.temp_db.TemporaryDBManagerWarning)

        self._assert_db_does_not_exist(db_name_1)
        self._assert_db_does_not_exist(db_name_2)

    def test_drop_db_explicit(self):
        """Tests database manager cleans up DBs when cleanup() called."""
        db_name_1 = '%s_test_tempdb_%08x' % (pwd.getpwuid(os.getuid())[0],
                                             random.getrandbits(32),)
        db_name_2 = db_name_1 + '_2'

        dbm = self._init(add_cleanup=False)
        dbm.create_db('my_pool_1', db_name=db_name_1)
        dbm.create_db('my_pool_2', db_name=db_name_2)

        dbm.cleanup()

        self._assert_db_does_not_exist(db_name_1)
        self._assert_db_does_not_exist(db_name_2)

    def test_cleanup_twice_safe(self):
        """Tests cleanup() can be called multiple times safely."""
        db_name_1 = '%s_test_tempdb_%08x' % (pwd.getpwuid(os.getuid())[0],
                                             random.getrandbits(32),)
        db_name_2 = db_name_1 + '_2'

        dbm = self._init(add_cleanup=False)
        dbm.create_db('my_pool_1', db_name=db_name_1)
        dbm.create_db('my_pool_2', db_name=db_name_2)

        dbm.cleanup()
        dbm.cleanup()

        self._assert_db_does_not_exist(db_name_1)
        self._assert_db_does_not_exist(db_name_2)

    def _delete_db(self, db_name):
        creator_pool = shared.db.get_rw_pool('temporary_db_creator')
        with creator_pool.transaction() as c:
            c.execute('DROP DATABASE %s' % (db_name,))

    def test_no_drop_db(self):
        """DBs instantiated with drop_db=False are not deleted."""
        db_name_1 = '%s_test_tempdb_%08x' % (pwd.getpwuid(os.getuid())[0],
                                             random.getrandbits(32),)
        db_name_2 = db_name_1 + '_2'

        dbm = self._init(add_cleanup=False)
        dbm.create_db('my_pool_1', db_name=db_name_1)

        self.addCleanup(self._delete_db, db_name_2)
        with warnings.catch_warnings(record=True) as emitted_warnings:
            dbm.create_db('my_pool_2', db_name=db_name_2, drop_db=False)
        self.assertEqual(len(emitted_warnings), 1)
        self.assertRegexpMatches(str(emitted_warnings[0].message),
                         'will not be dropped')
        self.assertEqual(emitted_warnings[0].category,
                         shared.db.temp_db.TemporaryDBManagerWarning)

        dbm.cleanup()

        self._assert_db_does_not_exist(db_name_1)

        # Make sure db_name_2 exists.
        pool_2 = shared.db.get_ro_pool('my_pool_2')
        self._assert_db_pool_is_db_name(pool_2, db_name_2)

    def test_no_rw_pool(self):
        """If DB created with rw_pool=False, no r/w pool created."""
        self._dbm = self._init()
        self._dbm.create_db('my_pool', rw_pool=False)
        with self.assertRaises(shared.db.errors.DBConfigurationError):
            shared.db.get_rw_pool('my_pool')

    def test_no_ro_pool(self):
        """If DB created with ro_pool=False, no r/o pool created."""
        self._dbm = self._init()
        self._dbm.create_db('my_pool', ro_pool=False)
        with self.assertRaises(shared.db.errors.DBConfigurationError):
            shared.db.get_ro_pool('my_pool')

    def test_basic_wait_for_replication(self):
        """Tests that wait_for_replication() uses a table to check for lag."""
        self._dbm = self._init()
        self._dbm.create_db('my_pool')

        with shared.db.get_ro_pool('my_pool').transaction() as c:
            c.execute('SELECT * FROM wait_for_replication')
            token_row = c.fetchone()

        self.assertIsNotNone(token_row)

        self._dbm.wait_for_replication('my_pool')

        with shared.db.get_rw_pool('my_pool').transaction() as c:
            c.execute('SELECT * FROM wait_for_replication')
            rw_token_row = c.fetchone()
        self.assertIsNotNone(rw_token_row)

        with shared.db.get_ro_pool('my_pool').transaction() as c:
            c.execute('SELECT * FROM wait_for_replication')
            ro_token_row = c.fetchone()
        self.assertIsNotNone(ro_token_row)

        self.assertEqual(rw_token_row, ro_token_row)
        self.assertNotEqual(ro_token_row, token_row)


class TestWaitForReplication(TestTempDBBase):

    """Test that we can properly handle slave lag.

    This tests that if various SQL statements are not propagated to the slave
    that we detect this as slave lag, and don't raise exceptions.

    We don't actually test that if there is no slave lag everything works
    properly, since that's tested implicitly above.
    """

    def setUp(self):
        TestTempDBBase.setUp(self)
        self._dbm = self._init()

        self._dbm._REPLICATION_CHECK_TRIES = 2
        self._dbm._REPLICATION_CHECK_SLEEP_TIME = 0.001

        # Create only a r/w pool so we don't implicitly test wait for
        # replication, since we want this test to tell us *why* it's failing.
        # If it's failing here, we can't run the tests...
        self._dbm.create_db('my_pool', ro_pool=False)

        master_pool = shared.db.get_rw_pool('my_pool')
        self._master_pool = master_pool

    def test_db_does_not_exist(self):
        """If DB create lags, ensure r/o pool works when it appears."""

        db_name = '%s_tempdb_db_dne_%08x' % (pwd.getpwuid(os.getuid())[0],
                                             random.getrandbits(32),)

        # We'll actually create a r/w pool here because we *don't* want to
        # worry about replication lag -- we're simulating it by dropping the
        # DB.  We won't read from this pool.
        slave_pool = self._dbm._create_pool(db_name, read_write=True)

        # Make sure that _wait_for_expected_token() returns False instead of
        # raising an exception.
        self.assertEqual(
            False,
            self._dbm._wait_for_expected_token(slave_pool, 'some_token'))

        # In the TempDB code, the pool is created when the DB may not exist,
        # but it must work when the DB is created.
        self._dbm.create_db(db_name=db_name, pool_name='new_pool',
                            ro_pool=False)
        with shared.db.get_rw_pool('new_pool').transaction() as cursor:
            cursor.execute('CREATE TABLE foo (a int) Engine=InnoDB')
            cursor.execute('INSERT INTO foo VALUES (42)')

        # The DB now exists on the master. We should be able to use our
        # slave_pool.
        with slave_pool.transaction() as cursor:
            cursor.execute('SELECT * FROM foo')
            self.assertItemsEqual(cursor.fetchall(), [(42,)])

    def test_table_does_not_exist(self):
        """Verify that a missing table counts as 'not replicated'."""

        token = self._dbm._setup_replication_check(self._master_pool)

        with self._master_pool.transaction() as cursor:
            cursor.execute('DROP TABLE `wait_for_replication`')

        # Master pool should have 'slave lag', since we're simulating the table
        # being missing.
        self.assertEqual(
            False,
            self._dbm._wait_for_expected_token(self._master_pool, token))

    def test_table_is_empty(self):
        """Verify that an empty table counts as 'not replicated'."""

        token = self._dbm._setup_replication_check(self._master_pool)

        with self._master_pool.transaction() as cursor:
            cursor.execute('DELETE FROM `wait_for_replication`')

        # Master pool should have 'slave lag', since we're simulating the table
        # being missing.
        self.assertEqual(
            False,
            self._dbm._wait_for_expected_token(self._master_pool, token))

    def test_table_is_wrong(self):
        """Verify that wrong value in the table counts as 'not replicated'."""

        token = self._dbm._setup_replication_check(self._master_pool)

        with self._master_pool.transaction() as cursor:
            cursor.execute('DELETE FROM `wait_for_replication`')
            cursor.execute('INSERT INTO `wait_for_replication` (value) '
                           'VALUES (%s)', ('badval',))

        # Master pool should have 'slave lag', since we're simulating the table
        # being missing.
        self.assertEqual(
            False,
            self._dbm._wait_for_expected_token(self._master_pool, token))
