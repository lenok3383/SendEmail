"""Connection pool stuff.

:Status: $Id: //prod/main/_is/shared/python/db/dbcp/pool.py#19 $
:Authors: jwescott, ohmelevs
"""

import contextlib
import logging
import threading
import time

from shared.db import errors


class ConnectionPool(object):
    """Database connection pool class.

    This class should not be directly instantiated.
    """

    def __init__(self, max_size, conn_factory, default_timeout, idle_timeout):
        """ConnectionPool initialization.

        :Parameters:
        - max_size: Maximum size of the pool.
        - conn_factory: Connection factory object.
        - default_timeout: Default timeout to wait for a connection.
        - idle_timeout: Timeout to close idle connection.
        """
        self._log = logging.getLogger('shared.db')
        self._max_size = max_size
        self._default_timeout = default_timeout
        self._idle_timeout = idle_timeout
        self._lock = threading.Condition()
        self._idle_pool = set()
        self._active_pool = set()
        self.conn_factory = conn_factory

    def __del__(self):
        """Shuts down the connection pool."""
        self.shutdown()

    @contextlib.contextmanager
    def connection(self, timeout=None):
        """Connection context.

        :Parameters:
        - timeout: Timeout to get a connection.
                   None means default timeout will be used.
        """
        connection = self._get_connection(timeout)
        try:
            with self.conn_factory.error_translation():
                yield connection
        finally:
            self._place_back_connection(connection)

    @contextlib.contextmanager
    def transaction(self, timeout=None, cursor_class=None):
        """Transaction context.

        :Parameters:
        - timeout: Timeout to get a connection.
        - cursor_class: Cursor class.
        """
        with self.connection(timeout) as conn:
            cursor = conn.cursor(cursor_class)
            cursor.execute('BEGIN')
            try:
                yield cursor
                cursor.execute('COMMIT')
            except Exception:
                cursor.execute('ROLLBACK')
                raise
            finally:
                cursor.close()

    def num_idle(self):
        """Returns the number of idle (available) connections in the pool."""
        return len(self._idle_pool)

    def num_active(self):
        """Returns the number of pooled connections that are in use."""
        return len(self._active_pool)

    def is_available(self):
        """Returns True if the number of available connections is > 0."""
        return self.num_idle() > 0 or self.num_active() < self._max_size

    def is_in_use(self, conn):
        """Returns True if the passed-in connection is active (i.e. --
        can be used).
        """
        return conn in self._active_pool

    def shutdown(self):
        """Shuts down the connection pool gracefully, closing all
        connections to the database.

        This method will block until all active connections are returned to
        the pool.
        """
        with self._lock:
            while self.num_active() > 0:
                self._lock.wait()
            for conn, last_use in self._idle_pool:
                try:
                    conn.close()
                except Exception as err:
                    self._log.error('Error has occurred during closing a '
                                    'connection: %s', err)
            self._idle_pool = set()

    def close_idle_connections(self):
        """Closes idle connections by idle timeout from configuration file.
        If no idle timeout is configured do nothing.
        """
        if not self._idle_timeout:
            return
        with self._lock:
            now = time.time()
            for conn, last_use in set(self._idle_pool):
                if (now - last_use) > self._idle_timeout:
                    self._log.debug('Closing idle connection %s', conn)
                    try:
                        conn.close()
                    except Exception as err:
                        self._log.error('Error has occurred during closing a '
                                        'connection: %s', err)
                    self._idle_pool.remove((conn, last_use))
            self._lock.notify()

    def _get_connection(self, timeout=None):
        """Returns a connection from the pool of available connections.

        If none are available, this method will block until a connection
        becomes available or until the given timeout (in seconds) has been
        exceeded.

        :Parameters:
        - timeout: wait time for a connection.

        :Raises:
        - TimeoutError: If it was impossible to get a valid connection
                        until the given (or default) timeout has been
                        exceeded.
        - DBError: If DB related error has been occurred during creation of a
                   new connection.

        :Returns: Database connection object.
        """
        if timeout is None:
            timeout = self._default_timeout
        with self._lock:
            for i in xrange(timeout):
                if self.is_available():
                    break
                else:
                    self._lock.wait(1)
            if not self.is_available():
                raise errors.TimeoutError(timeout)

            while self.num_idle() > 0:
                conn = self._idle_pool.pop()[0]
                if self.conn_factory.is_valid(conn, timeout):
                    break
                else:
                    self._log.debug('Connection: %s from the idle pool is '
                                    'invalid.', conn)
            else:
                conn = self.conn_factory.get_conn()
                self._log.debug('Opened new connection: %s', conn)

            self._active_pool.add(conn)
            return conn

    def _place_back_connection(self, conn):
        """Returns connection to the pool.

        :Parameters:
        - conn: connection to place back.
        """
        with self._lock:
            self._active_pool.remove(conn)
            self._idle_pool.add((conn, time.time()))
            self._lock.notify()
            self._log.debug('Connection %s is returned to the pool.', conn)


