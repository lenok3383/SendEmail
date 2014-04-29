"""Base connection factory stuff.

:Status: $Id: //prod/main/_is/shared/python/db/dbcp/connection_factory.py#17 $
:Authors: ohmelevs
"""

import contextlib
import logging
import os
import time


class BaseConnectionFactory(object):
    """DB connection factories should extend this class."""

    READ_WRITE_MODE = 'rw'
    READ_ONLY_MODE = 'ro'

    def __init__(self, config, pool_name, read_write=False):
        """Create a new connection factory from the configuration.

        Subclasses of BaseConnectionFactory will be responsible for parsing
        needed parameters from the configuration.

        :Parameters:
            - `config`: standard configuration object
            - `pool_name`: name of section to use
            - `read_write`: True if read/write connections should be made,
              False if read-only
        """
        self.pool_name = pool_name
        if read_write:
            self.mode = self.READ_WRITE_MODE
        else:
            self.mode = self.READ_ONLY_MODE

    def is_valid(self, conn, timeout=None):
        """Returns True if conn is a valid database connection that can
        be used for queries.  If conn is deemed to be invalid, it should
        be closed here.
        """
        return False

    def get_conn(self):
        """Return a DB-API database connection to the underlying DBMS."""
        raise NotImplementedError

    def error_translation(self):
        """Context to translate into DB non-specific exceptions."""
        raise NotImplementedError


class BaseCursor(object):
    """Base cursor class.  Concrete cursor must be inherited from this class.
    Appends tag from the environment variable as a query comment if such
    exists.
    """

    _log_query = False

    def __init__(self, *args, **kwargs):
        self._sql_tag = os.environ.get('APP_SQL_TAG')
        super(BaseCursor, self).__init__(*args, **kwargs)

    def execute(self, query, args=None):
        """Execute query with given args."""
        with execute_log_context(query, args, self._log_query):
            return self._execute_impl(self._append_sql_tag(query), args)

    def executemany(self, query, args):
        """Executemany query with given args."""
        with executemany_log_context(self._log_query):
            return self._executemany_impl(self._append_sql_tag(query), args)

    def _execute_impl(self, query, args):
        """This method must be implemented in a subclass."""
        raise NotImplementedError

    def _executemany_impl(self, query, args):
        """This method must be implemented in a subclass."""
        raise NotImplementedError

    def _append_sql_tag(self, query):
        """Appends tag to the given query from APP_SQL_TAG env var."""
        if self._sql_tag is None:
            return query
        elif query.endswith(';'):
            # MySQL gets confused if we append a comment after a semi-colon,
            # leading to "Commands out of sync" error.
            return query
        else:
            return '%s -- %s' % (query, self._sql_tag)


@contextlib.contextmanager
def execute_log_context(query, args, log_query=False):
    """Logs errors that occurs during query execution and successfully
    executed queries if configured.

    :Parameters:
    - query: query to log.
    - args: query arguments.
    - log_query: Log all queries flag. Default: False.

    Note: We initialize the logger at runtime as any logger initialized at
    import time will get disabled by Diablo's call to
    logging.config.fileConfig() if a log.conf is used.
    """
    start_time = time.time()
    try:
        yield
    except Exception as error:
        # Add query information to the exception instance so we
        # won't lose it during translation to DBError subclasses.
        error.query = query
        error.query_args = args
        raise
    finally:
        if log_query:
            logger = logging.getLogger()
            logger.info('Cursor execution:\n'
                         '\tquery=%s\n'
                         '\targs=%s\n'
                         '\ttime=%d ms',
                         query,
                         args,
                         (time.time() - start_time) * 1000)


@contextlib.contextmanager
def executemany_log_context(log_query=False):
    """Logs executemany failures and start/end if configured.

    :Parameters:
    - log_query: Log start/end flag. Default: False.

    Note: We initialize the logger at runtime as any logger initialized at
    import time will get disabled by Diablo's call to
    logging.config.fileConfig() if a log.conf is used.
    """
    start_time = time.time()
    if log_query:
        logger = logging.getLogger()
        logger.debug('Execute many is called.')
    try:
        yield
    finally:
        if log_query:
            logger.info('Execute many is finished and took: %d ms:',
                         (time.time() - start_time) * 1000)
