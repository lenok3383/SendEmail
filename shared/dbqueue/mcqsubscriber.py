"""DB MultiCast Queue exceptions.

:Status: $Id: //prod/main/_is/shared/python/dbqueue/mcqsubscriber.py#6 $
:Author: vburenin
"""

import cPickle

from MySQLdb import ProgrammingError

from shared.db.utils import retry
from shared.dbqueue import errors
from shared.dbqueue import utils
from shared.dbqueue.mcqbase import MulticastQueueBase

class MulticastQueueSubscriber(MulticastQueueBase):

    """A multicast queue subscriber."""

    def __init__(self, queue_name, pointer_name, status_writer_db_pool,
                 data_reader_db_pool):
        """A multicast queue subscriber.

        There can only be one of these on any
        given database with a certain queue_name/pointer name combination since
        that is the primary key in the queue_state table.
        Subscribers pop data off of a queue managed but the subscriber and are
        able to pop elements from the queue, seek backwards to a specific
        position or to a certain time and to the end of the queue.

        :param queue_name: an unique name for the queue.
        :param pointer_name: AKA subscriber name. Value should be the
                the same for all application subscribers to read it in sequence
                order.
        :param status_writer_db_pool: a db pool to provide connections to
               database hosting queue_state table.
        :param data_reader_db_pool: Connection pool to the publisher
               database where all queue data is stored.
        """
        MulticastQueueBase.__init__(self, queue_name, pointer_name,
                                    status_writer_db_pool)

        self.__last_ts = 0
        self._data_reader_db_pool = data_reader_db_pool

    @retry
    def pop(self, num_desired_objects):
        """Read a list of object from the queue.

        :param num_desired_objects: The maximum number of objects you
                                    want to read from the queue
        :return: a list of objects from the queue.
        """
        with self._writer_conn_pool.transaction() as sub_rw_cursor:
            with self._data_reader_db_pool.transaction() as data_ro_cursor:
                return self.__popper(sub_rw_cursor, data_ro_cursor,
                                     num_desired_objects)

    @retry
    def seek(self, new_pos=-1):
        """Move the read pointer for the subscriber.

        :param new_pos: if it is -1, seeks to the end of the queue;
                else, if it is 0, seeks to the head of the queue;
                else, it is a tell() tuple.
        :exception: Throw OutOfRangeError is the given new_pos is
                invalid, either too old or too new.
        """

        with self._writer_conn_pool.transaction() as sub_rw_cursor:
            with self._data_reader_db_pool.transaction() as data_ro_cursor:
                return self.__seeker(sub_rw_cursor, data_ro_cursor, new_pos)

    @retry
    def seek_to_ts(self, ts):
        """Move the read pointer based on time stamp.

        Moves the read pointer for the subscriber to based on supplied
        timestamp in seconds.

        :param ts: The timestamp to seek to.  Read pointer is moved to the
        start of the first table (pos_id=0) whose first data element has an
        mtime wich is less than ts.  If no table meets this criteria, the read
        pointer is moved to the first table.
        """
        with self._writer_conn_pool.transaction() as sub_rw_cursor:
            with self._data_reader_db_pool.transaction() as data_ro_cursor:
                return self.__seeker_to_ts(sub_rw_cursor, data_ro_cursor, ts)

    @retry
    def init_queue_state(self):
        """Call this method before you start reading the data from the queue.

        This method must be called first time. All next calls will be just
        ignored.
        """

        with self._writer_conn_pool.transaction() as sub_rw_cursor:
            with self._data_reader_db_pool.transaction() as data_ro_cursor:
                return self.__try_to_insert_queue_state(sub_rw_cursor,
                                                        data_ro_cursor)

    @retry
    def read_stat(self):
        """Returns queue read status such as items and seconds behind.

        This operation is heavy enough, so, don't call it too often.

        :return: tuple(items behind, seconds behind)
        """
        with self._writer_conn_pool.transaction() as sub_rw_cursor:
            with self._data_reader_db_pool.transaction() as data_ro_cursor:
                return self.__read_stats(sub_rw_cursor, data_ro_cursor)

    def __get_publisher_state(self, data_ro_cursor):
        return self._get_state_from_db(data_ro_cursor, 'in')

    def __popper(self, sub_rw_cursor, data_ro_cursor, num_items):
        """Logic for the pop function."""
        items = []
        num_needed = num_items

        # Get subscriber info.  This locks the transaction until completed.
        sub_stat = self._get_state_from_db(sub_rw_cursor, lock=True)
        if not sub_stat:
            raise errors.InvalidQueueStatusError(
                'No status information for the queue subscriber')
        pos_id = sub_stat.pos_id
        sub_table_name = sub_stat.table_name
        sub_table_number = utils.get_table_num(sub_table_name)

        # Get publisher info.
        pub_stat = self.__get_publisher_state(data_ro_cursor)
        if not pub_stat:
            return items
        last_table_num = utils.get_table_num(pub_stat.table_name)

        # It is edge case when there are not data in the database.
        if pub_stat.pos_id == 0 and last_table_num == 0:
            return items

        while True:
            num_new_items = 0
            try:
                last_pos_id, data = self.__select_data(data_ro_cursor,
                                                       num_needed,
                                                       sub_table_name,
                                                       pos_id)
                num_new_items = len(data)
                if num_new_items:
                    items.extend([cPickle.loads(obj) for obj in data])
                    pos_id = last_pos_id
            except ProgrammingError:

                # That table was probably purged...ignore and try the next
                # table in the sequence.

                pass

            if len(items) < num_items and sub_table_number < last_table_num:
                pos_id = 0
                sub_table_number += 1
                sub_table_name = self._get_table_name(sub_table_number)
                num_needed = num_items - len(items)
            else:

                # Stop when we have the right number of items or we've gone
                # to the end of the queue.

                break

        self._update_queue_state(sub_rw_cursor, pos_id, sub_table_name,
                                 sub_stat.odometer + len(items))

        return items

    def __seeker(self, sub_rw_cursor, data_ro_cursor, new_pos):
        """Logic for seeking to a new read location using position."""
        # Get the current position and lock it.
        table_name, pos_id, odometer = self._get_state_from_db(
                                            sub_rw_cursor, lock=True)[1:4]

        if new_pos == -1:

            # Get publisher info which is the last possible
            # table name and pos_id.

            table_name, pos_id = self.__get_publisher_state(data_ro_cursor)[1:3]
        elif not new_pos:
            sorted_tables = self._get_sorted_data_tables(data_ro_cursor)
            if sorted_tables:
                table_name = sorted_tables[0]
                pos_id = 0
        else:
            table_name, pos_id = new_pos
            # Make sure it's a valid seek position.
            try:
                max_count = self._get_last_pos_id(data_ro_cursor, table_name)
                if pos_id > max_count:
                    raise errors.OutOfRangeError(
                    'pos_id: %s, max seekable position for table %s: %s' % (
                                pos_id, table_name, max_count))
            except errors.EmptyTableError:
                raise errors.OutOfRangeError(
                    'Table %s does not exist' % (table_name,))

        # Seek to position.
        self._update_queue_state(sub_rw_cursor, pos_id, table_name, odometer)

    def __seeker_to_ts(self, sub_rw_cursor, data_ro_cursor, ts):
        """Logic for seeking to a new read location using timestamp."""
        # Assert the ts is valid.
        try:
            ts = float(ts)
        except ValueError:
            raise TypeError('Timestamp must be a number')

        sorted_tables = self._get_sorted_data_tables(data_ro_cursor,
                                                     reverse=True)
        if not sorted_tables:
            return

        # Get the current position and lock it.
        table_name, _, odometer = self._get_state_from_db(
                                            sub_rw_cursor, lock=True)[1:4]


        # Go to the first table whose first element is older than the ts.  If
        # that never happens, just go to the very first table.

        for table_name in sorted_tables:
            first_ts = self._get_first_ts(data_ro_cursor, table_name)
            if first_ts < ts:
                break

        self._update_queue_state(sub_rw_cursor, 0, table_name, odometer)

    def __try_to_insert_queue_state(self, sub_rw_cursor, data_ro_cursor):
        """Update existing queue_state information or
           add new record for the queue.
        """
        data_tables = self._get_sorted_data_tables(data_ro_cursor)
        if not data_tables:
            # There is no data in the queue state for the reader.
            self._insert_queue_state(sub_rw_cursor, self._get_table_name())
        else:

            # There are some rows in queue_state.  Set to the first one
            # because this should be called when creating a publisher.

            self._insert_queue_state(sub_rw_cursor, data_tables[0])

    def __select_data(self, data_ro_cursor, num_of_items, table_name, pos_id):
        query = 'SELECT pos_id, data FROM %s WHERE pos_id > %d LIMIT %d'
        args = (table_name, pos_id, num_of_items)
        data_ro_cursor.execute(query % args)
        rows = data_ro_cursor.fetchall()
        if rows:

            # There is some odd behavior where MySQLdb will return array
            # objects instead of strings.

            try:
                return rows[-1][0], [r[1].tostring() for r in rows]
            except AttributeError:
                return rows[-1][0], [r[1] for r in rows]
        else:
            return 0, []

    def __read_stats(self, sub_rw_cursor, data_ro_cursor):
        sub_stat = self._get_state_from_db(sub_rw_cursor)
        pub_stat = self.__get_publisher_state(data_ro_cursor)
        tables = self._get_sorted_data_tables(data_ro_cursor)

        sub_ts = 0
        if tables and pub_stat:
            sql = """SELECT mtime FROM %s WHERE pos_id >= %%s
                     ORDER BY pos_id LIMIT 1""" % (sub_stat.table_name,)
            if data_ro_cursor.execute(sql, (sub_stat.pos_id,)) > 0:
                sub_ts = utils.mtime_to_ts(data_ro_cursor.fetchone()[0])

        # Get all of the tables in front of the subscriber.
        try:
            tables_in_front = tables[tables.index(sub_stat.table_name):]
        except ValueError:

            # sub_stat is probably out of range of the currently available
            # rollover tables.  count all the rollover tables as tables in
            # front.

            tables_in_front = tables

        # Get the number of rows in front of current position.
        items_in_front = 0
        for table_name in tables_in_front:
            items_in_front += self._get_last_pos_id(data_ro_cursor, table_name)

        return (items_in_front - sub_stat.pos_id,
                int(pub_stat.timestamp - sub_ts))
