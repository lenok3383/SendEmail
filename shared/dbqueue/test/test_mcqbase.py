"""MulticastQueueBase class tests.

:Status: $Id: //prod/main/_is/shared/python/dbqueue/test/test_mcqbase.py#5 $
:Author: vburenin
"""

import unittest2
from datetime import datetime

from shared.dbqueue import errors, mcqbase
from shared.dbqueue.mcqbase import MulticastQueueBase
from shared.dbqueue.test import mockobjects
from shared.testing.vmock import matchers
from shared.testing.vmock import mockcontrol

TEST_APP_NAME = 'mcqbase_test'

class CreateMulticastQueueTest(unittest2.TestCase):

    def setUp(self):
        self.mc = mockcontrol.MockControl()
        self.pool_mock = mockobjects.ConnectionPoolMock(self.mc)
        self.cursor = self.pool_mock.cursor_mock

    def test_create_multicast_queue_if_tbl_exists(self):
        """Check if CREATE query is executed when table already exists."""
        self.cursor.execute(matchers.str_with('SHOW TABLES LIKE')).returns(1)
        self.cursor.fetchone().returns(('queue_state',))
        self.mc.replay()
        mcqbase.create_multicast_queue(self.pool_mock)
        self.mc.verify()

    def test_create_multicast_queue_if_tbl_do_not_exist(self):
        """Check if CREATE query is executed when table does not exist."""
        self.cursor.execute(matchers.str_with('SHOW TABLES LIKE')).returns(0)
        self.cursor.fetchone().returns(None)
        self.cursor.execute(matchers.str_with('CREATE TABLE')).returns(0)
        self.mc.replay()
        mcqbase.create_multicast_queue(self.pool_mock)
        self.mc.verify()


class MulticastQueueBaseTest(unittest2.TestCase):

    def setUp(self):
        self.q_name = 'test_queue_name'
        self.p_name = 'test_pointer_name'
        self.mc = mockcontrol.MockControl()
        self.pool_mock = mockobjects.ConnectionPoolMock(self.mc)
        self.cursor = self.pool_mock.cursor_mock
        self.mqb = MulticastQueueBase(self.q_name, self.p_name, self.pool_mock)

    def test_status(self):
        """status() calls an appropriate method"""
        self.mc.mock_method(self.mqb, '_get_state_from_db')\
            (self.cursor).returns('done')

        self.mc.replay()
        self.assertEqual('done', self.mqb.status())
        self.mc.verify()

    def test_get_all_data_tables_info(self):
        """get_all_data_tables_info() calls an appropriate method"""
        self.mc.mock_method(self.mqb, '_get_all_data_tables_info_db')\
            (self.cursor).returns('done')

        self.mc.replay()
        self.assertEqual('done', self.mqb.get_all_data_tables_info())
        self.mc.verify()

    def test_get_sorted_data_tables_reverse(self):
        """Check if _get_sorted_data_tables() using reverse argument right way."""
        self.cursor.execute(matchers.str_with('SHOW TABLES LIKE'))
        self.cursor.fetchall().returns((('tablename_1',),('tablename_2',)))
        self.mc.replay()

        self.assertEqual(['tablename_2', 'tablename_1'],
                        self.mqb._get_sorted_data_tables(self.cursor,
                        reverse=True))
        self.mc.verify()

    def test_get_sorted_data_tables(self):
        """Check if _get_sorted_data_tables() using reverse argument."""
        self.cursor.execute(matchers.str_with('SHOW TABLES LIKE'))
        self.cursor.fetchall().returns((('tablename_1',),('tablename_2',)))
        self.mc.replay()

        self.assertEqual(['tablename_1', 'tablename_2'],
                        self.mqb._get_sorted_data_tables(self.cursor,
                        reverse=False))
        self.mc.verify()

    def test_tell(self):
        """tell() calls status() and cut result to return just [1:3] vals"""
        self.mc.mock_method(self.mqb, 'status')().returns((0, 1, 2, 3, 4))

        self.mc.replay()
        self.assertEqual((1, 2), self.mqb.tell())
        self.mc.verify()

    def test_tell_exception(self):
        """Check if exception is generated if there is no status"""
        self.mc.mock_method(self.mqb, 'status')().returns(None)

        self.mc.replay()
        self.assertRaises(errors.InvalidQueueStatusError, self.mqb.tell)
        self.mc.verify()

    def test_get_table_name(self):
        """Table name is built correctly."""
        self.assertEqual(self.mqb._get_table_name(),
                          '%s_%s' % (self.q_name, 0))

    def test_get_all_data_tables_info_db(self):
        """Check if status of all tables is taken.

        It also tests _get_data_tables and _get_sorted_data_tables.
        """

        t1 = self.q_name + '_1'
        t2 = self.q_name + '_101'
        t3 = self.q_name + '_11'
        get_last_pos_mock = self.mc.mock_method(self.mqb, '_get_last_pos_id')
        get_size_mock = self.mc.mock_method(self.mqb, '_get_obj_sizes_db_func')

        self.cursor.execute("SHOW TABLES LIKE 'test\\_queue\\_name\\_%'")
        self.cursor.fetchall().returns(((t1,), (t2,), (t3,)))

        get_size_mock(self.cursor, t1).returns((1,2))
        get_last_pos_mock(self.cursor, t1).returns(1)

        get_size_mock(self.cursor, t3).returns((5,6))
        get_last_pos_mock(self.cursor, t3).returns((3))

        get_size_mock(self.cursor, t2).returns((3,4))
        get_last_pos_mock(self.cursor, t2).returns(2)


        self.mc.replay()
        actual = self.mqb._get_all_data_tables_info_db(self.cursor)
        expected = [('test_queue_name_1', 1, 1, 2),
                    ('test_queue_name_11', 3, 5, 6),
                    ('test_queue_name_101', 2, 3, 4)]

        self.assertEqual(expected, actual)
        self.mc.verify()

    def test_get_first_ts(self):
        """Appropriate SQL query should be executed to get first table ts"""
        self.cursor.execute(matchers.str_with('t_1 ORDER BY pos_id ASC'))
        self.cursor.fetchone().returns((datetime(2011, 10, 1),))

        self.mc.replay()
        self.assertEqual('1317452400.0',
                         str(self.mqb._get_first_ts(self.cursor, 't_1')))
        self.mc.verify()

    def test_get_last_ts(self):
        """Appropriate SQL query should be executed to get last table ts"""
        self.cursor.execute(matchers.str_with('t_1 ORDER BY pos_id DESC'))\
            .returns(1)
        self.cursor.fetchone().returns((datetime(2011, 10, 1),))

        self.mc.replay()
        self.assertEqual('1317452400.0',
                         str(self.mqb._get_last_ts(self.cursor, 't_1')))
        self.mc.verify()

    def test_get_last_pos_id(self):
        """Appropriate SQL query should be executed to get last table pos id"""
        self.cursor.execute(matchers.str_with('t_1 ORDER BY pos_id DESC'))\
            .returns(1)
        self.cursor.fetchone().returns((1,))

        self.mc.replay()
        self.assertEqual(1, self.mqb._get_last_pos_id(self.cursor, 't_1'))
        self.mc.verify()

    def test_get_obj_sizes_db_func(self):
        """Check if an SQL query is built to get object sizes."""
        self.cursor.execute(matchers.str_with('min(length(data)) FROM t_1'))\
            .returns(1)

        self.cursor.fetchone().returns((1, 2))

        self.mc.replay()
        self.assertEqual((1, 2),
                          self.mqb._get_obj_sizes_db_func(self.cursor, 't_1'))
        self.mc.verify()

    def test_get_obj_sizes_db_func_none(self):
        """Check if appropriate exception generated if there is no table."""
        self.cursor.execute(matchers.str_with('min(length(data)) FROM t_1'))\
            .returns(0)

        self.mc.replay()

        self.assertRaises(errors.EmptyTableError,
                          self.mqb._get_obj_sizes_db_func,
                          self.cursor,
                          't_1')
        self.mc.verify()

    def test_get_state_from_db_default(self):
        """Table should not be locked to take status"""
        self.cursor.execute(matchers.str_without('FOR UPDATE'),
                            (self.q_name, self.p_name))
        self.cursor.fetchone().returns(
                (1, 2, 3, 4, datetime(2011, 1, 1, 1)))
        self.mc.replay()

        expected = mcqbase.State(1, 2, 3, 4, 1293872400.0)
        actual = self.mqb._get_state_from_db(self.cursor)
        self.assertEqual(expected, actual)

        self.mc.verify()

    def test_get_state_from_db_lock(self):
        """Table should be locked to take status"""
        self.cursor.execute(matchers.str_with('FOR UPDATE'),
                            (self.q_name, self.p_name))
        self.cursor.fetchone().returns(
                (1, 2, 3, 4, datetime(2011, 1, 1, 1)))

        self.mc.replay()
        expected = mcqbase.State(1, 2, 3, 4, 1293872400.0)
        actual = self.mqb._get_state_from_db(self.cursor, lock=True)
        self.assertEqual(expected, actual)
        self.mc.verify()

    def test_get_state_from_db_differ_pointer(self):
        """SQL query should be executed with custom pointer name."""
        self.cursor.execute(matchers.str_without('FOR UPDATE'),
                            (self.q_name, 'name'))
        self.cursor.fetchone().returns(
                (1, 2, 3, 4, datetime(2011, 1, 1, 1)))

        self.mc.replay()
        expected = mcqbase.State(1, 2, 3, 4, 1293872400.0)
        actual = self.mqb._get_state_from_db(self.cursor, pointer_name='name')
        self.assertEqual(expected, actual)

        self.mc.verify()

    def test_insert_queue_state(self):
        """INSERT sql query should be executed with 't1' reader name"""
        self.cursor.execute(matchers.str_with('INSERT IGNORE'),
                            (self.q_name, self.p_name, 't1', 0))
        self.mc.replay()
        self.mqb._insert_queue_state(self.cursor, 't1')
        self.mc.verify()

    def test_update_queue_state(self):
        """UPDATE sql query should be executed with appropriate params."""
        self.cursor.execute(matchers.str_with('UPDATE queue_state'),
                            (1, 't1', 2, self.q_name, self.p_name))
        self.mc.replay()
        self.mqb._update_queue_state(self.cursor, 1, 't1', 2)
        self.mc.verify()


if __name__ == "__main__":
    unittest2.main()
