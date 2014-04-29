"""MySQL connection factory implementation.

:Status: $Id: //prod/main/_is/shared/python/db/dbcp/mysql.py#17 $
:Authors: jwescott, ohmelevs
"""

import contextlib
import threading

import MySQLdb

from MySQLdb.converters import conversions, string_literal
from MySQLdb.cursors import Cursor as MySQLdbCursor

from shared.db import errors
from shared.db.dbcp.connection_factory import BaseConnectionFactory, BaseCursor


# MySQL error codes.
WRONG_CREDENTIALS = 1045
WRONG_HOST = 2005
UNKNOWN_DATABASE = 1049

CON_COUNT = 1040
HOST_IS_BLOCKED = 1129
HOST_NOT_PRIVILEGED = 1130
ABORTING_CONNECTION = 1152
NET_READ_ERROR_FROM_PIPE = 1154
NEW_ABORTING_CONNECTION = 1184
TOO_MANY_USER_CONNECTIONS = 1203
CONNECTION = 2002
CONN_HOST = 2003
SERVER_LOST = 2013
NAMEDPIPE_CONNECTION = 2015
INVALID_CONN_HANDLE = 2048

NO_REFERENCED_ROW = 1216

LOCK_WAIT_TIMEOUT = 1205
LOCK_DEADLOCK = 1213

CONNECTION_ERRORS = [CON_COUNT, HOST_IS_BLOCKED, HOST_NOT_PRIVILEGED,
                     ABORTING_CONNECTION, NET_READ_ERROR_FROM_PIPE,
                     NEW_ABORTING_CONNECTION, TOO_MANY_USER_CONNECTIONS,
                     CONNECTION, CONN_HOST, SERVER_LOST, NAMEDPIPE_CONNECTION,
                     INVALID_CONN_HANDLE]

CONFIG_ERRORS = [WRONG_CREDENTIALS, WRONG_HOST, UNKNOWN_DATABASE]

ERRORS_TO_RETRY = [LOCK_WAIT_TIMEOUT, LOCK_DEADLOCK]


DEFAULT_CHARSET = 'utf8'


class MySQLConnectionFactory(BaseConnectionFactory):
    """MySQL connection factory."""

    def __init__(self, config, pool_name, read_write=False):
        """Create a new MySQL connection factory using the given
        configFile and dbConfigName.

        :Parameters:
        - config: shared.db.config.Config object
        - pool_name: The name of the section in the Config
        - read_write: True: read/write; False (default): read only.

        :Execptions:
        - `DBConfigurationError`: raised when required parameters are missing.
        """
        super(MySQLConnectionFactory, self).__init__(config, pool_name,
                                                     read_write)

        self.db_name = config.get('%s.db' % (pool_name,))
        self.host = config.get('%s.%s.host' % (pool_name, self.mode))
        self.user = config.get('%s.%s.user' % (pool_name, self.mode))
        self.passwd = config.get('%s.%s.password' % (pool_name, self.mode))
        self.connect_timeout = config.get('%s.connect_timeout' % (pool_name,),
                                          0)
        self.auto_commit = config.get('%s.auto_commit' % (pool_name,))
        self.isolation_level = config.get('%s.isolation_level' % (pool_name,))
        self.session_options = config.get('%s.session_options' % (pool_name,),
                                          dict())
        self.charset = config.get('%s.charset' % (pool_name,), DEFAULT_CHARSET)
        if config.get('%s.log_queries' % (pool_name,)):
            self.cursor_class = MySQLLoggableCursor
        else:
            self.cursor_class = MySQLCursor

        # Check for missing parameters.
        if not self.db_name or not self.host or not self.user:
            pool_type = 'ro'
            if read_write:
                pool_type = 'rw'

            raise errors.DBConfigurationError(
                'Missing configuration parameters for %s db pool for %s db' %
                (pool_type, pool_name))

    def is_valid(self, conn, timeout=None):
        """Validate a connection in the individual thread with the given
        timeout."""
        validator = _MySQLConnectionValidator(conn)
        validator.start()
        validator.join(timeout)
        return validator.valid()

    def get_conn(self):
        """Get a connection to the MySQL database."""
        with self.error_translation():
            conn = MySQLdb.connect(db=self.db_name,
                                   host=self.host,
                                   user=self.user,
                                   passwd=self.passwd,
                                   compress=1,
                                   connect_timeout=self.connect_timeout,
                                   cursorclass=self.cursor_class)
            try:
                self._setup_conn(conn)
            except Exception as err:
                # If we've already opened a connection and got an exception,
                # we're just trying to close a connection.
                try:
                    conn.close()
                except:
                    pass
                raise err
            return conn

    @contextlib.contextmanager
    def error_translation(self):
        """Error translation context.

        Catch MySQL specific operational exceptions and translate it into the
        general database exceptions.

        :Raises:
        - DBConnectionError
        - DBRetryError
        - DBConfigurationError
        """
        try:
            yield
        except MySQLdb.OperationalError as err:
            if err[0] in CONNECTION_ERRORS:
                raise errors.DBConnectionError(err)
            elif err[0] in ERRORS_TO_RETRY:
                raise errors.DBRetryError(err)
            elif err[0] in CONFIG_ERRORS:
                raise errors.DBConfigurationError(err)
            else:
                raise

    def _setup_conn(self, conn):
        """Sets up given connection."""
        sql = list()
        if self.auto_commit:
            sql.append('AUTOCOMMIT = %s' % (self.auto_commit,))
        if self.isolation_level:
            sql.append('SESSION TRANSACTION ISOLATION LEVEL %s' %
                       (self.isolation_level,))
        for variable, value in self.session_options.iteritems():
            sql.append('SESSION %s=%s' % (variable, value,))
        sql.append('NAMES %s' % (self.charset,))
        cursor = conn.cursor()
        try:
            cursor.execute('SET ' + ', '.join(sql))
        finally:
            cursor.close()


class _MySQLConnectionValidator(threading.Thread):
    """MySQL connection validator."""

    def __init__(self, conn):
        threading.Thread.__init__(self, name='_MySQLConnectionValidator')
        self._conn = conn
        self._valid = False

    def run(self):
        """Run method for validation."""
        try:
            self._conn.ping()
            # ping will throw exception if fails, so this won't get run:
            self._valid = True
        except MySQLdb.OperationalError:
            # this is the normal ping-failed result, _valid already False
            self._conn.close()

    def valid(self):
        """Returns validation result."""
        return self._valid


class MySQLCursor(BaseCursor, MySQLdbCursor):
    """MySQLdb cursor."""

    def _execute_impl(self, query, args=None):
        """MySQLdb execute implementation."""
        return MySQLdbCursor.execute(self, query, args)

    def _executemany_impl(self, query, args):
        """MySQLdb executemany implementation."""
        return MySQLdbCursor.executemany(self, query, args)


class MySQLLoggableCursor(MySQLCursor):
    """MySQLdb cursor with logging of the queries."""

    _log_query = True


class InClause(list):
    """
    This class will turn sequences into valid in clauses when passed in as an
    SQL arguments.

    Code snippet:

        sql = "SELECT * FROM my_table WHERE ctime = %s AND name IN %s"

        my_in_clause = InClause()
        my_in_clause.append('testing')
        my_in_clause.append('trying')

        args = (90673980, my_in_clause,)

        conn = my_pool.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, args)
        results = cursor.fetchall()
        cursor.close()
        conn.close()

    SQL sent to MySQL:

        SELECT * FROM my_table
        WHERE ctime = 90673980 AND name IN ('testing','trying')
    """
    pass


def __convert_in_clause(iterable, conv_mapping):
    """In clause converter for MySQLdb module."""
    default_conv = MySQLdb.converters.escape
    converted = list()
    for i in iterable:
        conv = conv_mapping.get(i.__class__, default_conv)
        converted.append(conv(i, conv_mapping))
    return '(' + ','.join(converted) + ')'


class LikeClause(str):
    """Type class for a like clause the LikeClause2Str function will
    escape the like clause except for like clause specific special characters:

        %   Matches any number of characters, even zero characters
        _   Matches exactly one character
        \\%  Matches one "%" character
        \\_  Matches one "_" character

    The default escape character is "\\".
    """
    escape_char = '\\'


def __convert_like_clause(chars, conv_mapping):
    """Like clause converter for MySQLdb module."""
    converted = list()

    escape = '\\'
    if hasattr(chars, 'escape_char'):
        escape = chars.escape_char

    escape_flag = False
    for char in chars:
        if char == escape:
            converted.append(escape * 2)
            escape_flag = not escape_flag
        elif char == "'":
            converted.append(escape + char)
            escape_flag = False
        elif char in ('n', 'r', 't'):
            if escape_flag:
                converted[-1] = escape
            converted.append(char)
            escape_flag = False
        else:
            converted.append(char)
            escape_flag = False

    if escape_flag:
        converted.append(escape * 2)

    if not escape == "\\":
        return "'%s' ESCAPE %s" % (''.join(converted),
                                   string_literal(escape))
    else:
        return "'%s'" % (''.join(converted),)


conversions[InClause] = __convert_in_clause
conversions[LikeClause] = __convert_like_clause
