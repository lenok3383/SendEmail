"""A base class for multicast queue variants.

It's mostly full of helper functions.
Don't instantiate this directly.

:Status: $Id: //prod/main/_is/shared/python/dbqueue/mcqbase.py#6 $
:Author: brianz, ted, vburenin
"""

import collections
import re

from MySQLdb import ProgrammingError
from shared.db.utils import retry
from shared.dbqueue import errors
from shared.dbqueue import utils


State = collections.namedtuple('State', ['queue_name', 'table_name',
                                         'pos_id', 'odometer', 'timestamp'])

def create_multicast_queue(writer_conn_pool):
    """Creates multicast queue main table.

    :param writer_conn_pool: Connection poll with RW permissions.
    """
    show_query = """SHOW TABLES LIKE 'queue_state'"""
    query = """CREATE TABLE IF NOT EXISTS queue_state (
        `queue_name` VARCHAR(32) NOT NULL COMMENT 'the name of the queue',
        `pointer_name` VARCHAR(32) COMMENT 'special value "in" for writer pointer, all other value for read pointers',
        `table_name` VARCHAR(32) NOT NULL COMMENT 'table name of queue_name_xxxx',
        `pos_id` INT DEFAULT 0 COMMENT 'pos_id in side the table',
        `odometer` BIGINT DEFAULT 0 COMMENT 'tracking for number of entries',
        `mtime` TIMESTAMP NOT NULL,
        PRIMARY KEY (`queue_name`, `pointer_name`)
        ) ENGINE=InnoDB"""

    with writer_conn_pool.transaction() as cursor:
        cursor.execute(show_query)
        if cursor.fetchone() is None:
            cursor.execute(query)


class MulticastQueueBase(object):

    """A base class for multicast queue variants.

    It's mostly full of helper functions.
    Don't instantiate this directly.
    """

    def __init__(self, queue_name, pointer_name, writer_conn_pool):


        # Make sure the queue_name is valid MySQL table name.
        if re.search('[^a-z0-9_]', queue_name):
            raise ValueError('Invalid queue_name. '
                             'Valid characters are [a-z0-9_].')

        object.__init__(self)

        self._queue_name = queue_name
        self._pointer_name = pointer_name
        self._writer_conn_pool = writer_conn_pool

    @retry
    def status(self):
        """Current status.

        :return: tuple (queue_name, table_name, pos_id, odometer, mtime)
        """
        with self._writer_conn_pool.transaction() as cursor:
            return self._get_state_from_db(cursor)

    @retry
    def get_all_data_tables_info(self):
        """Returns list of table info.

        :return: [('table_name', last_pos, min_size, max_size)...]
        """

        with self._writer_conn_pool.transaction() as cursor:
            return self._get_all_data_tables_info_db(cursor)

    def tell(self):
        """Tells current table name and position in that table.

        :return: (table_name, pos_id)
        """
        stat = self.status()
        if stat:
            return stat[1:3]
        else:
            raise errors.InvalidQueueStatusError(
                'Does not have information for the queue ' \
                'in queue_status table')

    def init_queue_state(self):
        """Init current queue state.

        Call this method before you start reading/pushing the data to/from the
        queue. This method must be called first time. All next calls will be
        just ignored.
        """
        raise NotImplementedError('This method must be implemented')

    def _get_table_name(self, table_num=0):
        return '%s_%s' % (self._queue_name, table_num)

    def _get_sorted_data_tables(self, cursor, reverse=False):
        """Returns list of all the current queue table names."""

        # Queue name may contain underscores, which SQL treats as any char.
        # So, it should be filtered separately.

        sql_qname = self._queue_name.replace('_', '\\_') + '\\_%'
        cursor.execute("""SHOW TABLES LIKE '%s'""" % (sql_qname,))

        tables = [r[0] for r in cursor.fetchall()]
        tables.sort(key=utils.get_table_num, reverse=reverse)
        return tables

    def _get_all_data_tables_info_db(self, cursor):
        tables = self._get_sorted_data_tables(cursor)
        table_info = []
        for table_name in tables:
            try:
                obj_sizes = self._get_obj_sizes_db_func(cursor, table_name)
            except errors.EmptyTableError:
                obj_sizes = (None, None)
            table_info.append((table_name,
                    self._get_last_pos_id(cursor, table_name),
                    obj_sizes[0], obj_sizes[1]))

        return table_info

    def _get_first_ts(self, cursor, table_name):
        query = 'SELECT mtime FROM %s ORDER BY pos_id ASC LIMIT 1'
        return utils.mtime_to_ts(self.__execute(cursor, query, table_name))

    def _get_last_ts(self, cursor, table_name):
        query = 'SELECT mtime FROM %s ORDER BY pos_id DESC LIMIT 1'
        return utils.mtime_to_ts(self.__execute(cursor, query, table_name))

    def _get_last_pos_id(self, cursor, table_name):
        query = 'SELECT pos_id FROM %s ORDER BY pos_id DESC LIMIT 1'
        return self.__execute(cursor, query, table_name)

    def _get_obj_sizes_db_func(self, cursor, table_name):
        query = 'SELECT max(length(data)), min(length(data)) FROM %s'
        return self.__execute(cursor, query, table_name)

    def _get_state_from_db(self, cursor, pointer_name=None, lock=False):
        query = """SELECT queue_name, table_name, pos_id, odometer, mtime
                   FROM queue_state
                   WHERE queue_name = %s AND pointer_name = %s"""

        if lock:
            query = '%s FOR UPDATE' % (query,)

        if pointer_name is None:
            pointer_name = self._pointer_name
        cursor.execute(query, (self._queue_name, pointer_name,))

        data = cursor.fetchone()
        if data is None:
            return None

        data = data[0:-1] + (utils.mtime_to_ts(data[-1]),)
        return State(*data)

    def _insert_queue_state(self, cursor, table_name, pos_id=0):
        query = """INSERT IGNORE INTO queue_state
                   (queue_name, pointer_name, table_name, pos_id)
                   VALUES (%s, %s, %s, %s)"""

        args = (self._queue_name, self._pointer_name, table_name, pos_id)
        cursor.execute(query, args)

    def _update_queue_state(self, cursor, pos_id, table_name, odometer):
        query = """UPDATE queue_state
                   SET pos_id = %s, table_name = %s, odometer = %s
                   WHERE queue_name = %s
                   AND pointer_name = %s"""
        args = (pos_id, table_name, odometer,
                self._queue_name, self._pointer_name)

        cursor.execute(query, args)

    def __execute(self, cursor, query, table_name):
        try:
            if cursor.execute(query % (table_name)) == 0:
                raise errors.EmptyTableError('Table %s is empty' % \
                                             (table_name,))
        except ProgrammingError:
            # Raise exception if there is no such table.
            raise errors.NoSuchTableError('Table %s does not exist' % \
                                          (table_name,))
        data = cursor.fetchone()
        if len(data) == 1:
            return data[0]
        return data
