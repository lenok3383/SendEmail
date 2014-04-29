"""DB MultiCast Queue exceptions.

:Status: $Id: //prod/main/_is/shared/python/dbqueue/mcqpublisher.py#7 $
:Author: vburenin
"""

import cPickle
import threading
import time

from MySQLdb import ProgrammingError

from shared.db.utils import retry
from shared.dbqueue import errors
from shared.dbqueue import utils
from shared.dbqueue.mcqbase import MulticastQueueBase

PROTOCOL = 0

class MulticastQueuePublisher(MulticastQueueBase):

    """A multicast queue publisher."""

    def __init__(self, queue_name,
                writer_conn_pool,
                rollover_seconds,
                auto_purge,
                purge_callback,
                purge_threshold_hours):
        """A multicast queue publisher.

        There can only be one of these on any given database with a certain
        queue_name.  In the database, it's stored with a unique key of
        queue_name and pointer_name = 'in'. It is the publishers job to put
        new data onto the queue.  It keeps track of it's current write
        position and will usually purge old data assuming it's told to.

        :param queue_name: an unique name for the queue
        :param write_conn_pool: a db pool to provide connections to
                database hosting queue_state and queue_data tables.
                It must implement get_conn() method.
        :param queue_purge_threshold_hours: defines how long can the
                data stay in the queue. Its unit is hours and it can
                be a float or an int.
        :param auto_purge: Defines whether the publisher will
                call purge() automatically. If there are extra
                heavy things that need to be cleaned up before dropping
                queue_data partitions, it is recommended to set it
                to False and specify purge_callback. In that case,
                purge() should be invoked from other daemon processes
                or crons.
        :param purge_callback: provide a callback object to perform
                extra clean up before the data object is removed
                from the queue. The callback will be invoked with
                a list of data objects that will be removed soon.
                If the callback return False, the purge() will be
                interrupted.
        :param rollover_seconds: The number of seconds to wait to create
                a new queue_state table when inserting data using the
                publisher.  If the delta between now() and the timestamp
                of the last data item is greater that this value, a new
                data table will be created and new data will end up there.
        """

        # Base class.  Publisher uses 'in' pointer name to publish data.
        # This name is unique and must not be used by subscribers.
        MulticastQueueBase.__init__(self, queue_name, 'in', writer_conn_pool)

        self._rollover_secs = rollover_seconds
        self.__purge_callback = purge_callback

        # Purging variables.
        self.__auto_purge = auto_purge
        self._purge_frequency = purge_threshold_hours * 3600
        self._last_purge_ts = 0

        self._table_name_to_ts = dict()
        self._table_name_to_ts_lock = threading.RLock()

    def __len__(self):
        """The length of the entire queue across all tables.

        :return: The length of the entire queue across all tables.
        """
        return self.__getlen()

    @retry
    def append(self, objects):
        """Append a list of object to the end of the queue

        :param objects: a list of serializable objects
        """
        with self._writer_conn_pool.transaction() as cursor:
            self.__appender(cursor, objects)

    @retry
    def purge(self, *args, **kwargs):
        """Purge all obsolete data tables."""
        with self._writer_conn_pool.transaction() as cursor:
            self.__purger(cursor, *args, **kwargs)

    @retry
    def init_queue_state(self):
        """Call this method before you start pushing data to the queue.

        This method must be called first time. All next calls will be just
        ignored.
        """

        with self._writer_conn_pool.transaction() as cursor:
            self.__try_to_insert_queue_state(cursor)

    @retry
    def __create_new_table(self, table_name):
        with self._writer_conn_pool.transaction() as cursor:
            self.__create_new_table_db_func(cursor, table_name)

    @retry
    def __getlen(self):
        with self._writer_conn_pool.transaction() as cursor:
            return self.__len(cursor)

    def __len(self, cursor):
        """Logic to figure out the total size of the queue."""
        _len = 0
        for table in self._get_sorted_data_tables(cursor):
            _len += int(self._get_last_pos_id(cursor, table))
        return _len

    def __appender(self, cursor, objects):
        """Logic for appending data into the queue."""
        if not objects:
            return

        now = time.time()

        # Get the current stats.
        publisher_state = self._get_state_from_db(cursor, lock=True)

        table_name = publisher_state.table_name

        # Check if it's time to create a new table.
        if self.__should_insert_table(cursor, table_name, now):
            # Get next new table number and create it.
            table_number = utils.get_table_num(table_name) + 1
            table_name = self._get_table_name(table_number)
            self.__create_new_table(table_name)

        objs = [cPickle.dumps(obj, PROTOCOL) for obj in objects]

        # Insert the data in the appropriate table and update the queue state.
        self.__insert_data(cursor, table_name, objs)
        pos_id = cursor.lastrowid + cursor.rowcount - 1
        self._update_queue_state(cursor, pos_id, table_name,
                                 publisher_state.odometer + len(objects))
        if self.__auto_purge:
            self.__purger(cursor)

    def __purger(self, cursor, *args, **kwargs):
        """Default logic for purging old data tables."""

        # timestamp_threshold: cutoff point for tables to be stored/deleted.
        # All tables whose last entry has a timestamp before this will be
        # dropped.

        now_time = time.time()
        purge_timer = time.time() - self._last_purge_ts
        if purge_timer < min(60, self._purge_frequency / 10):
            return

        timestamp_threshold = now_time - self._purge_frequency

        # Update the ts since we are purging now.
        self._last_purge_ts = now_time

        # Get the current stats.
        publisher_state = self._get_state_from_db(cursor, lock=True)
        active_table = publisher_state.table_name

        # Get all of the tables except active one, that have the potential
        # to be purged.
        data_tables = self._get_sorted_data_tables(cursor)
        for table_name in data_tables:
            if table_name == active_table:
                continue
            try:
                last_ts = self._get_last_ts(cursor, table_name)
            except errors.EmptyTableError:
                last_ts = -1
            except errors.NoSuchTableError:
                # The table could be deleted by another thread/node.
                return

            if last_ts < timestamp_threshold:
                if self.__purge_callback is not None:
                    # Call the callback with list of items to be deleted.
                    try:
                        cursor.execute('SELECT data FROM %s' % (table_name,))
                        all_data = cursor.fetchall()
                    except ProgrammingError:
                        # The table could be deleted by another thread/node.
                        all_data = []

                    all_items = []
                    if all_data:
                        try:
                            # There is some odd behavior where MySQLdb will
                            # return array objects instead of strings.
                            all_data = [it[0].tostring() for it in all_data]
                        except AttributeError:
                            all_data = [it[0] for it in all_data]

                        for item in all_data:
                            try:
                                all_items.append(cPickle.loads(item))
                            except cPickle.UnpicklingError:
                                # Can't unpickle data. Ignore this item.
                                pass

                    if not self.__purge_callback(all_items):
                        return

                cursor.execute('DROP TABLE IF EXISTS %s' % (table_name,))
                self._table_name_to_ts_lock.acquire()
                if table_name in self._table_name_to_ts:
                    del self._table_name_to_ts[table_name]
                self._table_name_to_ts_lock.release()

    def __try_to_insert_queue_state(self, cursor):
        """Creates a new queue_state publisher record if none exist."""
        data_tables = self._get_sorted_data_tables(cursor)
        if data_tables:
            if self._get_state_from_db(cursor) is None:

                # There are db table but no valid rows in queue_status
                # try to fix the queue_status table.

                table_name = data_tables[-1]
                pos_id = self._get_last_pos_id(cursor, table_name)
                self._insert_queue_state(cursor, table_name, pos_id)
        else:
            # There is no data in the queue state for the publisher.
            self._insert_queue_state(cursor, self._get_table_name())

    def __create_new_table_db_func(self, cursor, table_name):
        query = """CREATE TABLE IF NOT EXISTS %s (
              pos_id INT AUTO_INCREMENT COMMENT 'Auto-incremental queue position.',
              data BLOB COMMENT 'The cPickled object',
              mtime TIMESTAMP NOT NULL,
              PRIMARY KEY (pos_id)
            ) ENGINE=InnoDB"""
        cursor.execute(query % (table_name,))

    def __insert_data(self, cursor, table_name, objs):
        query = 'INSERT INTO %s (data) VALUES (%%s)' % (table_name,)
        cursor.executemany(query, objs)

    def __make_insert_table_decision(self, first_ts, ref_time):
        return (ref_time - first_ts > self._rollover_secs) and first_ts

    def __should_insert_table(self, cursor, table_name, ref_time):
        # See if it's time to insert a new data table.

        self._table_name_to_ts_lock.acquire()
        try:
            if table_name in self._table_name_to_ts:
                first_ts = self._table_name_to_ts[table_name]
                return self.__make_insert_table_decision(first_ts, ref_time)

            first_ts = 0
            try:
                first_ts = self._get_first_ts(cursor, table_name)
                self._table_name_to_ts[table_name] = first_ts
            except errors.NoSuchTableError:
                # The table doesn't exist.
                return True
            except errors.EmptyTableError:
                # It exists but is empty.
                return False
        finally:
            self._table_name_to_ts_lock.release()

        return self.__make_insert_table_decision(first_ts, ref_time)
