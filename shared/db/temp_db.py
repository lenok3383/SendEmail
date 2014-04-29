"""Creates temporary databases and database pools.

This module is mostly meant for creating temporary databases in automated
testing.

Example usage:

class TestMyApp(unittest.TestCase):

    def setUp(self):
        config = shared.conf.config.Config(
            os.path.expanduser('~/.unittest-db.conf'))
        temp_db_mgr = shared.db.temp_db.TemporaryDBManager(config, 'unittest')
        self.addCleanup(temp_db_mgr.cleanup)
        self.addCleanup(shared.db.shutdown)

        temp_db_mgr.create_db('corpus', 'my_app_test.sql')

    def test_db(self):
        # Test can use shared.db.get_ro_pool('corpus') or
        # shared.db.get_rw_pool('corpus') to get the created temp DB pool.
        pass

:Author: duncan
:Status: $Id: //prod/main/_is/shared/python/db/temp_db.py#4 $
"""

import os
import pwd
import random
import subprocess
import time
import warnings

import MySQLdb
import MySQLdb.constants.ER

import shared.conf.config
import shared.db.dbcp
import shared.db.dbcp.pool
import shared.db.dbcp.pool_manager
import shared.db.dbcp.mysql

class TemporaryDBManagerWarning(Warning):

    pass


class TemporaryDBManager(object):

    _REPLICATION_CHECK_TRIES = 60
    _REPLICATION_CHECK_SLEEP_TIME = 0.05

    def __init__(self, config_obj, config_section):
        """Creates a temporary database manager.

        The provided `config_obj` and `config_section` should be a standard
        Config object with a section containing standard database parameters
        (as in a db.conf). Because temporary databases are created, the
        "db_name" parameter in this configuration object is optional.

        :Parameters:
           - `config_obj`: shared.conf.Config object containing database config
             info.
           - `config_section`: section in the config object containing the
             database parameters
        """

        self._conf_dict = config_obj.get_snapshot()
        self._conf_section = config_section
        self._dbs_to_drop = list()

        # We'll use the "mysql" database for the pool that creates databases,
        # since we have to pick a db.
        creator_config_dict = dict(self._conf_dict)
        creator_config_dict['%s.db' % (config_section,)] = 'mysql'

        self._creator_pool = shared.db.dbcp.pool.ConnectionPool(
            max_size=1,
            conn_factory=shared.db.dbcp.mysql.MySQLConnectionFactory(
                config=shared.conf.config.ConfigFromDict(creator_config_dict),
                pool_name=self._conf_section,
                read_write=True),
            default_timeout=10, idle_timeout=10)

        # Register the pool so the manager can handle idle connections
        # properly.
        shared.db.set_rw_pool('temporary_db_creator', self._creator_pool)

    def create_db(self, pool_name, sql_files=None, db_name=None, drop_db=True,
                  rw_pool=True, ro_pool=True):
        """Creates a temporary database and sets up DB pools.

        :Parameters:
            - `pool_name`: Name of created database pools in pool manager.
            - `sql_files`: List of SQL files to load into the DB.
            - `db_name`: Temporary database name (in MySQL).
            - `drop_db`: If False, database is not deleted at cleanup
              time. (Default: True)
            - `rw_pool`: If False, no read-write pool is created.
            - `ro_pool`: If False, no read-only pool is created.
        """
        if not db_name:
            db_name = '%s_tempdb_%08x' % (pwd.getpwuid(os.getuid())[0],
                                          random.getrandbits(32),)

        self._create_mysql_db(db_name)

        # Create a r/w pool regardless of whether it's requested, as we'll use
        # it to load data (in theory).
        writer_pool = self._create_pool(db_name, read_write=True)
        if rw_pool:
            shared.db.set_rw_pool(pool_name, writer_pool)

        if sql_files:
            # Strings are iterable, and lead to confusing error messages...
            assert not isinstance(sql_files, basestring), \
                'sql_files should be a list of paths to sql files'
            self._load_schema(writer_pool, sql_files)

        if drop_db:
            self._dbs_to_drop.append(db_name)
        else:
            # Since this (drop_db == False) is primarily for developers, warn
            # them that they'll need to clean up the DB.
            warnings.warn(
                'Database %s will not be dropped automatically.'
                'Be sure to remove it yourself.' % (db_name,),
                TemporaryDBManagerWarning, stacklevel=2)

        if ro_pool:
            # Create a reader pool, make sure it's ready to go, and then put it
            # into the shared db pool cache.  We can't put it in the cache
            # until replication is verified because the db might not exist.
            reader_pool = self._create_pool(db_name, read_write=False)
            self._wait_for_replication(writer_pool, reader_pool, pool_name)
            shared.db.set_ro_pool(pool_name, reader_pool)

    @shared.db.retry
    def _create_mysql_db(self, db_name):
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore',
                'Can\'t drop database .*; database doesn\'t exist',
                MySQLdb.Warning)

            with self._creator_pool.transaction() as cursor:
                cursor.execute('CREATE DATABASE `%s`' % (db_name,))

    def _create_pool(self, db_name, read_write=True):
        pool_config_dict = dict(self._conf_dict)
        pool_config_dict['%s.db' % (self._conf_section,)] = db_name

        max_size = self._conf_dict.get(
            self._conf_section + '.pool_size',
            shared.db.dbcp.pool_manager.DEFAULT_MAX_CONNECTIONS)
        default_timeout = self._conf_dict.get(
            self._conf_section + '.default_timeout',
            shared.db.dbcp.pool_manager.DEFAULT_TIMEOUT)
        idle_timeout = self._conf_dict.get(
            self._conf_section + '.pool_size',
            shared.db.dbcp.pool_manager.DEFAULT_IDLE_TIMEOUT)

        return shared.db.dbcp.pool.ConnectionPool(
            max_size=max_size,
            conn_factory=shared.db.dbcp.mysql.MySQLConnectionFactory(
                config=shared.conf.config.ConfigFromDict(pool_config_dict),
                pool_name=self._conf_section,
                read_write=read_write),
            default_timeout=default_timeout, idle_timeout=idle_timeout)

    def _load_schema(self, rw_pool, sql_files):
        # Read sql_file
        sql_data = []
        for sql_file in sql_files:
            with open(sql_file, 'r') as fh:
                sql = fh.read()
                sql_data.append(sql)

        # TODO: We should handle loading the SQL directly instead of shelling
        # out to the mysql client.  Unfortunately, CREATE_DB.sql scripts
        # usually have mysql client-specific statements like "source", so we
        # would have to emulate the client a fair bit.
        #
        # We'll execute this from the directory containing the *first* sql file
        # passed in. This is often the CREATE_DB.sql, which is usually the
        # right place to start if we're going to see "source" directives.

        cmd_args = [
            'mysql', '-u', rw_pool.conn_factory.user,
            '--password=%s' % (rw_pool.conn_factory.passwd,),
            '-h', rw_pool.conn_factory.host, rw_pool.conn_factory.db_name]
        try:
            child = subprocess.Popen(
                cmd_args, close_fds=True, stdin=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=os.path.dirname(sql_files[0]))
            for sql in sql_data:
                child.stdin.write(sql)
            child.stdin.close()
            child.wait()
            if child.returncode != 0:
                raise Exception('MySQL returned %d: %s' % (child.returncode,
                                                       child.stderr.read()))
        except OSError, e:
            raise Exception('Failed to execute command: %s' % \
                            ' '.join(cmd_args))

    @shared.db.retry
    def _drop_db(self, db_name):
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore',
                'Can\'t drop database .*; database doesn\'t exist',
                MySQLdb.Warning)

            with self._creator_pool.transaction() as cursor:
                cursor.execute('DROP DATABASE IF EXISTS `%s`' % (db_name,))

    def cleanup(self):
        """Clean up created databases."""
        while self._dbs_to_drop:
            db_name = self._dbs_to_drop.pop()
            self._drop_db(db_name)

    def wait_for_replication(self, pool_name):
        """Wait for slave to catch up to master.

        In production-like environments, it is not reasonable to assume that a
        write can be read immediatedly after it is written. However, it is
        convenient in unit tests to write data and use it to test reading
        functions. This function can be used in these situations to wait until
        the data has propogated to the slave.

        This function should be used carefully, as it can be used to hide race
        conditions. Slave lag can lead to very subtle, yet serious bugs.
        """
        self._wait_for_replication(shared.db.get_rw_pool(pool_name),
                                   shared.db.get_ro_pool(pool_name),
                                   pool_name)

    def _is_same_host(self, *pools):
        hosts = set(pool.conn_factory.host for pool in pools)
        return bool(len(hosts) == 1)

    def _wait_for_replication(self, master_pool, slave_pool, pool_name,
                              force=False):

        if self._is_same_host(master_pool, slave_pool):
            # Same server for read-write and read-only, no slave lag.
            return

        token = self._setup_replication_check(master_pool)
        success = self._wait_for_expected_token(slave_pool, token)
        if not success:
            raise Exception('Too much slave lag for pool %s' % (pool_name,))

    def _setup_replication_check(self, master_pool):
        """Puts random value into the `wait_for_replication` table.

        The value in the table can then be read from the slave pool to see if
        it has propagated.

        :Returns: random value written to `wait_for_replication` table.
        """
        # The basic idea is here is we're going to write to a table and then
        # verify that the read is what we expected, retrying if necessary.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore',
                'Table .* already exists',
                MySQLdb.Warning)

            with master_pool.transaction() as cursor:
                cursor.execute('CREATE TABLE IF NOT EXISTS '
                               '  `wait_for_replication` (`value` char(8)) '
                               'Engine=InnoDB')

        token = '%08x' % (random.getrandbits(32),)

        with master_pool.transaction() as cursor:
            cursor.execute('DELETE FROM wait_for_replication')
            cursor.execute('INSERT INTO wait_for_replication (`value`) '
                           'VALUES (%s)', (token,))

        return token

    def _wait_for_expected_token(self, slave_pool, token):
        """Waits until all queries have propagated to the slave.

        This method will try check the `wait_for_replication` table repeatedly
        until it find `token` in the table. If, after checking a number of
        times, the expected data can not be read from the table, `False` is
        returned.

        :Returns: boolean indicating whether `token` was succefully read
        """
        # We need to be particularly careful in this function. `slave_pool`
        # might refer to a database that doesn't even exist yet, or the table
        # might not exist, or it might be empty, etc.

        for i in xrange(self._REPLICATION_CHECK_TRIES):
            found_row = None
            try:
                with slave_pool.transaction() as cursor:
                    cursor.execute('SELECT value FROM wait_for_replication')
                    found_row = cursor.fetchone()
            except MySQLdb.ProgrammingError, e:
                # Table doesn't exist yet.
                if e[0] != MySQLdb.constants.ER.NO_SUCH_TABLE:
                    raise
            except shared.db.errors.DBConfigurationError, e:
                # DB doesn't exist yet.
                if e[0] != MySQLdb.constants.ER.BAD_DB_ERROR:
                    raise
            if found_row and found_row[0] == token:
                return True
            time.sleep(self._REPLICATION_CHECK_SLEEP_TIME)

        return False

    def __del__(self):
        """Destructor calls cleanup().

        This object should be cleaned up using cleanup().  The destructor is
        only included as a failsafe.
        """
        if self._dbs_to_drop:
            warnings.warn(
                'TemporaryDBManager __del__ called. Be sure to call cleanup() '
                'explicitly to ensure databases are dropped.',
                TemporaryDBManagerWarning)
        self.cleanup()
