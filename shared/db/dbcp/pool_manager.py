"""Pool manager module.

:Status: $Id: //prod/main/_is/shared/python/db/dbcp/pool_manager.py#16 $
:Authors: jwescott, ohmelevs
"""

import logging
import os
import time
import threading

from shared import conf
from shared.db.dbcp.mysql import MySQLConnectionFactory
from shared.db.dbcp.pool import ConnectionPool
from shared.db.errors import DBConfigurationError, DBConnectionError


DEFAULT_TIMEOUT = 5
DEFAULT_MAX_CONNECTIONS = 20
DEFAULT_IDLE_TIMEOUT = 60

JOIN_TIMEOUT = 3


class DBPoolManagerImpl(object):
    """Database pool manager."""

    __conn_pools = dict()

    # Lock around instantiating missing pools in the dict.
    _conn_pools_lock = threading.RLock()

    def __init__(self, config_path=None):
        """Pool manager initialization.

        By default pool configuration is read from the 'db' config (usually in
        $CONFROOT/db.conf). A different configuration file can be passed in,
        for example for unit testing.

        :Parameters:
        - config_path: optional configuration file to override normal
          configuration (default read 'db' config from db.conf)
        """

        self._log = logging.getLogger('shared.db')

        self._config_path = config_path
        self._config = None

        self._idle_handler = _IdleConnectionHandler()

    def _make_pool(self, pool_name, read_write=False):

        # Lazy-load configuration and store it for consistency.
        if not self._config:
            if self._config_path:
                # Override for testing with a custom db.conf.
                config_obj = conf.config.Config(self._config_path)
            else:
                config_obj = conf.get_config('db')
            self._config = config_obj.get_snapshot()

        if pool_name not in self._config.sections():
            raise DBConfigurationError(
                'No configuration information for database %s' %
                (pool_name,))

        pool_size = self._config.get(pool_name + '.pool_size',
                               DEFAULT_MAX_CONNECTIONS)
        timeout = int(self._config.get(pool_name + '.timeout',
                                 DEFAULT_TIMEOUT))
        idle_timeout = self._config.get(pool_name + '.idle_timeout',
                                  DEFAULT_IDLE_TIMEOUT)

        pool = ConnectionPool(
            pool_size, _get_factory(self._config, pool_name, read_write),
            timeout, idle_timeout)
        self._set_pool(pool_name, pool, read_write)

    def get_rw_pool(self, pool_conf_name):
        """Returns the R/W database pool associated with the given name.

        :Parameters:
        - pool_conf_name: Pool name.
        - raise_exception: Raise exception or not if there is no such pool.

        :Raises:
        - DBConfigurationError: if there is no such rw pool.

        :Returns:
        ConnectionPool object.
        """
        return self._get_pool(pool_conf_name, True)

    def get_ro_pool(self, pool_conf_name):
        """Returns the R/O database pool associated with the given name.

        :Parameters:
        - pool_conf_name: Pool name.
        - raise_exception: Raise exception or not if there is no such pool.

        :Raises:
        - DBConfigurationError: if there is is no such ro pool.

        :Returns:
        ConnectionPool object.
        """
        return self._get_pool(pool_conf_name, False)

    def _get_pool(self, pool_name, read_write=False):
        """Get (or instantiate) a pool corresponding to the requested name."""
        pool_type = 'ro'
        if read_write:
            pool_type = 'rw'

        pool_key = '%s_%s' % (pool_name, pool_type)

        try:
            return DBPoolManagerImpl.__conn_pools[pool_key]
        except KeyError:
            pass

        # We don't want multiple calls to get_pool at the same time to create
        # multiple pools, so use this lock to serialize.
        with self._conn_pools_lock:
            pool = DBPoolManagerImpl.__conn_pools.get(pool_key)
            if pool:
                return pool
            else:
                self._make_pool(pool_name, read_write)
                return DBPoolManagerImpl.__conn_pools[pool_key]

    def set_rw_pool(self, pool_conf_name, pool):
        """Associates a provided R/W pool object with a given database name.
        Validates given pool.

        :Parameters:
        - pool_conf_name: Pool name.
        - pool: Connection pool object.
        """
        if self._validate_pool(pool):
            self._set_pool(pool_conf_name, pool, True)
        else:
            raise DBConfigurationError('Wrong pool is given: %s' % (pool,))

    def set_ro_pool(self, pool_conf_name, pool):
        """Associates a provided R/O pool object with a given database name.
        Validates given pool.

        :Parameters:
        - pool_conf_name: Pool name.
        - pool: Connection pool object.
        """
        if self._validate_pool(pool):
            self._set_pool(pool_conf_name, pool, False)
        else:
            raise DBConfigurationError('Wrong pool is given: %s' % (pool,))

    def validate_pools(self, rw_pools, ro_pools):
        """Loads and verifies the proper functioning of all the listed pools.

        This can be used at daemon start time (for example) to verify all of
        the needed database pools are properly configured.

        An exception will be raised if not all listed pools are valid.

        :Parameters:
        - rw_pools: list of the rw pools to validate.
        - ro_pools: list of the ro pools to validate.

        :Raises:
        - Exception if the database connections are not valid.
        """
        pools = list()
        if ro_pools:
            assert not isinstance(ro_pools, basestring), \
                'ro_pools must be a tuple or list'
            pools.extend(
                (name, 'ro', self.get_ro_pool(name)) for name in ro_pools)
        if rw_pools:
            assert not isinstance(rw_pools, basestring), \
                'rw_pools must be a tuple or list'
            pools.extend(
                (name, 'rw', self.get_rw_pool(name)) for name in rw_pools)

        for pool_name, pool_type, pool in pools:
            res = self._validate_pool(pool)
            if not res:
                # Sort of a catch all error... most real errors should have
                # thrown exceptions.
                raise DBConnectionError('Cannot validate %s pool %s' %
                                        (pool_type, pool_name))

    def clear_pool_cache(self):
        """Removes the cached pools."""
        for name, pool in DBPoolManagerImpl.__conn_pools.items():
            pool.shutdown()
            del DBPoolManagerImpl.__conn_pools[name]
        self._idle_handler.stop()

    def shutdown(self):
        """Stops pool manager."""
        self.clear_pool_cache()
        self._idle_handler.stop()

    @staticmethod
    def _get_pools():
        """Returns connection pool objects list."""
        return DBPoolManagerImpl.__conn_pools.values()

    def _set_pool(self, pool_conf_name, pool, read_write):
        """Sets connection pool.

        :Parameters:
        - pool_conf_name: Connection pool name.
        - pool: Connection pool object.
        - read_write: Connection pool mode.
        """
        if read_write:
            pool_type = 'Read-write'
            prefix = '_rw'
        else:
            pool_type = 'Read-only'
            prefix = '_ro'
        DBPoolManagerImpl.__conn_pools[pool_conf_name + prefix] = pool
        self._log.debug('%s pool %s is initialized.', pool_type,
                        pool_conf_name)
        self._idle_handler.start()

    def _validate_pool(self, pool_obj):
        """Checks if pool object is valid. This may raise an exception."""
        if pool_obj is None:
            return False

        with pool_obj.connection() as conn:
            return pool_obj.conn_factory.is_valid(conn)

class _IdleConnectionHandler(object):
    """Class to handle with idle connection in the individual thread."""

    def __init__(self, check_every=10):
        self._loop_sleep_tm = check_every
        self._log = logging.getLogger('shared.db')
        self._run = False
        self._worker = None
        self._worker_lock = threading.RLock()

    def start(self):
        """Start handler thread."""
        with self._worker_lock:
            if self._run:
                return
            self._worker = threading.Thread(target=self._handle,
                                            name='_IdleConnectionHandler')
            self._worker.setDaemon(True)
            self._run = True
            self._worker.start()
            self._log.debug('IdleConnectionHandler has been started.')

    def stop(self):
        """Stops handler thread."""
        with self._worker_lock:
            self._run = False
            if self._worker:
                self._worker.join(JOIN_TIMEOUT)

    def _handle(self):
        while self._run:
            pools = DBPoolManagerImpl._get_pools()
            if not pools:
                self._log.debug('There are no opened pools. Handler will be '
                                'stopped.')
                self._run = False
                break
            for pool in pools:
                try:
                    pool.close_idle_connections()
                except Exception:
                    self._log.exception('Problem checking pool: %s', pool)
            sec = 0
            while self._run and sec < self._loop_sleep_tm:
                time.sleep(0.1)
                sec += 0.1

    def __del__(self):
        try:
            self.stop()
        except:
            pass


def _get_factory(config, name, read_write):
    """Based on the database type and configuration file, get a
    connection factory.  If a new database type must be handled this method
    needs to be modified to allow DBCP to work with it.

    :Parameters:
    - config: shared.conf.config.Config object.
    - name: Database name.
    - read_write: True if the factory should have write access.

    :Returns:
    Subclass of BaseConnectionFactory.

    :Raises:
    - NotImplementedError: if dbtype is invalid.
    """
    dbtype = config[name + '.db_type']
    if dbtype.lower().find('mysql') != -1:
        return MySQLConnectionFactory(config, name, read_write)
    else:
        raise NotImplementedError('Unsupported DB type (for name "%s"): %s'
                                  % (name, dbtype))


