"""counter_stroage module. Define counter storage implementations in this module.

:Status: $Id $
:Authors: bjung, bwhitela
"""

import fcntl
import os
import re
import socket
import time
import xmlrpclib
import warnings

try:
    import sqlite3
except ImportError:
    # some python installations do not include sqlite
    pass

import shared.db.utils
import shared.monitor.counter


class StorageError(Exception):
    pass


class XMLRPCCounterStorage(shared.monitor.counter.BaseCounterStorage):

    """Counter storage class which sends counter values to a remote XMLRPC
    server."""

    def __init__(self, remote_addr, remote_method='post_counter',
                 max_buffer_size=1024):
        """Constructor.

        :param remote_host:
        """
        self.remote_host = remote_addr
        self.remote_method = remote_method
        self.buffer = []
        self.max_buffer_size = max_buffer_size
        self.proxy = xmlrpclib.ServerProxy(self.remote_host)

    def store(self, key, value, counter_name):
        """Send the counter value to the remote host.

        :param key: counter key
        :param value: counter value (integer)
        :param counter_name: counter name (may differ from key)

        :raises: StorageError
        """

        self.buffer.append((key, value, counter_name))
        while len(self.buffer) > self.max_buffer_size:
            self.buffer.pop(0)
        while True:
            try:
                success = getattr(self.proxy, self.remote_method)(*self.buffer[0])
            except socket.error as e:
                raise StorageError(str(e))
            if not success:
                raise StorageError('Counter storage failed')
            self.buffer.pop(0)
            if not self.buffer:
                break


class SQLiteCounterStorage(shared.monitor.counter.BaseCounterStorage):

    """Counter storage class which stores values in an SQLite db."""

    SUFFIX_ST = ':' + shared.monitor.counter.StaticCounter.suffix
    SUFFIX_TS = ':' + shared.monitor.counter.Timestamp.suffix
    SUFFIX_DYN = ':' + shared.monitor.counter.DynamicCounter.suffix
    RATE_RE = re.compile('.+:r(\d+)$')

    def __init__(self, sqlite_file_path, clear_contents=False):
        """Constructor.

        :param sqlite_file_path: full path to sqlite file, will be created if
        it doesn't exist.
        :param clear_contents: If True, deletes current contents of the db
              CAUTION: Do not specify this option if this method is called
                       while application is running.  Counts will be
                       lost until the next time the application writes the
                       counts.
        """

        self.sqlite_file_path = sqlite_file_path
        # lock may be opened, even if another process has lock
        file_lock = open(sqlite_file_path + '.lock', 'w')

        try:
            fcntl.flock(file_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                conn = sqlite3.connect(sqlite_file_path)
                with conn:
                    if clear_contents:
                        conn.execute('DROP TABLE IF EXISTS counts')

                    conn.execute('CREATE TABLE IF NOT EXISTS counts('
                                 'key TEXT, '
                                 'name TEXT, '
                                 'process_id INTEGER, '
                                 'value REAL, '
                                 'mtime INTEGER, '
                                 'PRIMARY KEY (key, process_id))')
            finally:
                fcntl.flock(file_lock, fcntl.LOCK_UN)
        except IOError, e:
            # pass if another process has lock
            if e.errno != 35:
                raise

    def store(self, key, value, counter_name):
        """Store the value to the database.

        :param key: counter key
        :param value: counter value (integer)
        :param counter_name: counter name (may differ from key)
        """

        process_id = os.getpid()
        conn = sqlite3.connect(self.sqlite_file_path)
        with conn:
            conn.execute('PRAGMA synchronous=0')
            conn.execute('INSERT OR REPLACE INTO counts('
                         'key, name, process_id, value, mtime'
                         ') values (?, ?, ?, ?, ?)',
                         (key, counter_name, process_id, value, int(time.time())))

    def aggregated_counts(self):
        """Reads counts from database and gives values aggregated from
        all processes.  Agregation is based on the suffix of the key name:
        Static Counters (:st) - Summed across processes
        Timestamp (:ts) - Most recent
        Dynamic Counters - The counter portion (:dyn) is summed, the rate portion
                           (:rXX) is averaged across processes.
        :return: Dict {<key_name>: <value>, ...}
        """
        conn = sqlite3.connect(self.sqlite_file_path)
        with conn:
            c = conn.execute('SELECT key, process_id, value FROM counts')
            res = c.fetchall()

        counts = {}
        for key, pid, value in res:
            key = str(key)
            if key in counts:
                counts[key].append(value)
            else:
                counts[key] = [value]

        aggr_counts = {}
        for key, values in counts.iteritems():
            if key.endswith(self.SUFFIX_ST) or key.endswith(self.SUFFIX_DYN):
                aggr_counts[key] = sum(values)
            elif key.endswith(self.SUFFIX_TS):
                # Sort timestamps and grab last
                aggr_counts[key] = sorted(values)[-1]
            elif self.RATE_RE.match(key):
                aggr_counts[key] = sum(values) / len(values)
            else:
                # Unknown key type, don't add it
                pass

        return aggr_counts


class MySQLCounterStorage(shared.monitor.counter.BaseCounterStorage):

    """Counter storage class which sends counter values to a MySQL DB."""

    def __init__(self, db_pool=None, node_name=None):
        """Constructor.

        :param db_pool: MySQL database writer connection pool.
        :param node_name: A name for this process in a possible cluster.
                          Default is <hostname>_<PID>. If your process restarts
                          the PID may change, adding new entries to the DB.
        """
        self._db_pool = db_pool
        self._pid = os.getpid()
        self._hostname = socket.gethostname()
        if node_name:
            self._node_name = node_name
        else:
            self._node_name = '%s_%s' % (self._hostname, self._pid)
        self._prepare_database()

    @shared.db.utils.retry
    def _prepare_database(self):
        """Prepare the table in the given db."""

        # If the table already exists the MySQL library is kind enough to print out
        # a warning, so catch that and ignore it.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore',
                'Table .* already exists')

            with self._db_pool.transaction() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS counters(
                   counter_name VARCHAR(255) BINARY NOT NULL COMMENT 'The name of the counter',
                   node_name VARCHAR(255) BINARY NOT NULL COMMENT 'The node name of the Diablo server',
                   value BIGINT COMMENT 'the long value',
                   mtime int COMMENT 'modified on',
                   mby VARCHAR(255) COMMENT 'modified by',
                   hostname VARCHAR(255) COMMENT 'hostname',
                   pid int COMMENT 'pid',
                   PRIMARY KEY (counter_name, node_name)
                ) ENGINE = InnoDB DEFAULT CHARSET=latin1;
                """)

    @shared.db.utils.retry
    def store(self, key, value, counter_name):
        """Send the counter value to the database.

        :param key: counter key
        :param value: counter value (integer)
        :param counter_name: counter name (may differ from key)
        """
        with self._db_pool.transaction() as cursor:
            cursor.execute("""INSERT
             INTO counters(counter_name, node_name, value, mtime, mby, hostname, pid)
             VALUES (%s, %s, %s, unix_timestamp(), %s, %s, %s)
             ON DUPLICATE KEY UPDATE
             value = values(value),
             mtime = unix_timestamp(),
             mby = values(mby),
             hostname = values(hostname),
             pid = values(pid)""",
             (key, self._node_name, value, counter_name, self._hostname, self._pid))
