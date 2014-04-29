"""MulticastQueueSubscriber class tests.

:Status: $Id: //prod/main/_is/shared/python/dbqueue/test/test_mcqsubscriber.py#5 $
:Author: vburenin
"""

import cPickle
import unittest2
from datetime import datetime

from shared.dbqueue import errors, mcqbase
from shared.dbqueue.mcqsubscriber import MulticastQueueSubscriber
from shared.dbqueue.test import mockobjects

from shared.testing.vmock import mockcontrol, matchers

TEST_APP_NAME = 'mcqsubscriber_test'

class Test(unittest2.TestCase):

    def setUp(self):
        self.q_name = 't'
        self.p_name = 'p'
        self.mc = mockcontrol.MockControl()
        self.rw_pool_mock = mockobjects.ConnectionPoolMock(self.mc)
        self.ro_pool_mock = mockobjects.ConnectionPoolMock(self.mc)
        self.rw_cursor = self.rw_pool_mock.cursor_mock
        self.ro_cursor = self.ro_pool_mock.cursor_mock
        self.mqs = MulticastQueueSubscriber(self.q_name, self.p_name,
                                            self.rw_pool_mock,
                                            self.ro_pool_mock)
        self.get_state_from_db_mock = self.mc.mock_method(self.mqs, '_get_state_from_db')
        self.update_queue_state_mock = self.mc.mock_method(self.mqs, '_update_queue_state')
        self.get_sorted_tables_mock = self.mc.mock_method(self.mqs, '_get_sorted_data_tables')
        self.get_last_pos_id_mock = self.mc.mock_method(self.mqs, '_get_last_pos_id')
        self.get_first_ts_mock = self.mc.mock_method(self.mqs, '_get_first_ts')

    def tearDown(self):
        self.mc.tear_down()

    def test_pop_from_one_table(self):
        """Test one record popped correctly."""
        # Defining mock sequence calls.
        self.get_state_from_db_mock(self.rw_cursor, lock=True) \
            .returns(mcqbase.State(self.q_name, 't_1', 1, 4,
                                   datetime(2011, 10, 11)))
        self.get_state_from_db_mock(self.ro_cursor, 'in') \
            .returns(mcqbase.State(self.q_name, 't_2', 4, 4,
                                   datetime(2011, 10, 11)))
        self.ro_cursor.execute(matchers.str_with('t_1 WHERE pos_id > 1 LIMIT 2'))

        self.ro_cursor.fetchall().returns(((2, cPickle.dumps('aa')),
                                        (3, cPickle.dumps('bb'))))

        self.update_queue_state_mock(self.rw_cursor, 3, 't_1', 6)

        # Run.
        self.mc.replay()
        self.assertEqual(['aa', 'bb'], self.mqs.pop(2))

        # Verify
        self.mc.verify()

    def test_pop_from_two_tables(self):
        """First table contains only 2 records, so to get three records
        another request should be made from second table.
        """
        self.get_state_from_db_mock(self.rw_cursor, lock=True) \
            .returns(mcqbase.State(self.q_name, 't_1', 1, 4,
                                   datetime(2011, 10, 11)))
        self.get_state_from_db_mock(self.ro_cursor, 'in') \
            .returns(mcqbase.State(self.q_name, 't_2', 4, 4,
                                   datetime(2011, 10, 11)))

        self.ro_cursor.execute(matchers.str_with('t_1 WHERE pos_id > 1 LIMIT 3'))
        self.ro_cursor.fetchall().returns(((1, cPickle.dumps('aa')),
                                        (2, cPickle.dumps('bb'))))

        self.ro_cursor.execute(matchers.str_with('t_2 WHERE pos_id > 0 LIMIT 1'))
        self.ro_cursor.fetchall().returns(((1, cPickle.dumps('cc')),))

        self.update_queue_state_mock(self.rw_cursor, 1, 't_2', 7)

        # Run.
        self.mc.replay()
        self.assertEqual(['aa', 'bb', 'cc'], self.mqs.pop(3))

        # Verify.
        self.mc.verify()

    def test_seek_to_end(self):
        """Seek pointer to the end of the queue."""
        self.get_state_from_db_mock(self.rw_cursor, lock=True) \
            .returns(mcqbase.State(self.q_name, 't_1', 1, 4,
                                   datetime(2011, 10, 11)))
        self.get_state_from_db_mock(self.ro_cursor, 'in') \
            .returns(mcqbase.State(self.q_name, 't_2', 4, 4,
                                   datetime(2011, 10, 11)))
        self.update_queue_state_mock(self.rw_cursor, 4, 't_2', 4)

        # Run.
        self.mc.replay()
        # Seek to end. Default new position is -1.
        self.mqs.seek()

        # Verify.
        self.mc.verify()

    def test_seek_to_head(self):
        """Seek pointer to the head of the queue."""
        self.get_state_from_db_mock(self.rw_cursor, lock=True) \
            .returns(mcqbase.State(self.q_name, 't_1', 1, 4,
                                   datetime(2011, 10, 11)))
        self.get_sorted_tables_mock(self.ro_cursor).returns(('t_1', 't_2'))
        self.update_queue_state_mock(self.rw_cursor, 0, 't_1', 4)

        # Run.
        self.mc.replay()
        # Seek to head of the queue.
        self.mqs.seek(0)

        # Verify.
        self.mc.verify()

    def test_seek_to_position(self):
        """Seek pointer to the specific position in the queue."""
        self.get_state_from_db_mock(self.rw_cursor, lock=True) \
            .returns(mcqbase.State(self.q_name, 't_1', 1, 4,
                                   datetime(2011, 10, 11)))
        self.get_last_pos_id_mock(self.ro_cursor, 't_2').returns(2)
        self.update_queue_state_mock(self.rw_cursor, 2, 't_2', 4)

        # Run and seek to the position
        self.mc.replay()
        self.mqs.seek(('t_2', 2))

        # Verify.
        self.mc.verify()

    def test_seek_to_bad_position_offset(self):
        """Test if seeking to incorrect position raises exception."""
        self.get_state_from_db_mock(self.rw_cursor, lock=True) \
            .returns(mcqbase.State(self.q_name, 't_1', 1, 4,
                                   datetime(2011, 10, 11)))
        self.get_last_pos_id_mock(self.ro_cursor, 't_2').returns(2)

        # Run and seek to the position
        self.mc.replay()
        self.assertRaises(errors.OutOfRangeError, self.mqs.seek, ('t_2', 5))

        # Verify.
        self.mc.verify()

    def test_seek_to_bad_position_table(self):
        """Seeking to bad position (table doesn't exists) raises exception."""
        self.get_state_from_db_mock(self.rw_cursor, lock=True) \
            .returns(mcqbase.State(self.q_name, 't_1', 1, 4,
                                   datetime(2011, 10, 11)))
        self.get_last_pos_id_mock(self.ro_cursor, 't_3') \
            .raises(errors.EmptyTableError())

        # Run and seek to the position
        self.mc.replay()
        self.assertRaises(errors.OutOfRangeError, self.mqs.seek, ('t_3', 2))

        # Verify.
        self.mc.verify()

    def test_seek_to_ts(self):
        """Seek to the position defined by time stamp only."""
        self.get_sorted_tables_mock(self.ro_cursor, reverse=True) \
            .returns(('t_3', 't_2', 't_1'))
        self.get_state_from_db_mock(self.rw_cursor, lock=True) \
            .returns(mcqbase.State(self.q_name, 't_1', 1, 4,
                                   datetime(2011, 10, 11)))

        self.get_first_ts_mock(self.ro_cursor, 't_3').returns(3)
        self.get_first_ts_mock(self.ro_cursor, 't_2').returns(2)
        self.get_first_ts_mock(self.ro_cursor, 't_1').returns(1)

        self.update_queue_state_mock(self.rw_cursor, 0, 't_1', 4)

        # Run.
        self.mc.replay()
        self.mqs.seek_to_ts(1)

        # Verify.
        self.mc.verify()

    def test_insert_queue_state_data_doesnt_exists(self):
        """Pointer data doesn't exist."""
        self.get_sorted_tables_mock(self.ro_cursor).returns(tuple())
        self.mc.mock_method(self.mqs, '_insert_queue_state') \
            (self.rw_cursor, 't_0')

        # Run.
        self.mc.replay()
        self.mqs.init_queue_state()

        # Verify.
        self.mc.verify()

    def test_insert_queue_state_data_exists(self):
        """Pointer data exists."""
        self.get_sorted_tables_mock(self.ro_cursor).returns(('t_1', 't_2'))
        self.mc.mock_method(self.mqs, '_insert_queue_state') \
            (self.rw_cursor, 't_1')

        # Run.
        self.mc.replay()
        self.mqs.init_queue_state()

        # Verify.
        self.mc.verify()

    def test_read_stats(self):
        """Test read stats"""
        self.get_state_from_db_mock(self.rw_cursor) \
            .returns(mcqbase.State(self.q_name, 't_0', 1, 4,
                                   1318239100))
        self.get_state_from_db_mock(self.ro_cursor, 'in') \
            .returns(mcqbase.State(self.q_name, 't_1', 1, 4,
                                   1318239200))
        self.get_sorted_tables_mock(self.ro_cursor).returns(('t_1', 't_2'))
        self.ro_cursor.execute(matchers.str_with('t_0'), (1,)).returns(1)
        self.ro_cursor.fetchone().returns((datetime(2011, 10, 10),))
        self.get_last_pos_id_mock(self.ro_cursor, 't_1').returns(1)
        self.get_last_pos_id_mock(self.ro_cursor, 't_2').returns(2)

        self.mc.replay()
        # Two items behind, 9200 seconds behind.
        self.assertEquals((2, 9200), self.mqs.read_stat())
        self.mc.verify()

    def test_read_stats_no_data(self):
        """No data in the queue, even data tables are not created."""
        self.get_state_from_db_mock(self.rw_cursor) \
            .returns(mcqbase.State(self.q_name, 't_0', 0, 0, 0))
        self.get_state_from_db_mock(self.ro_cursor, 'in') \
            .returns(mcqbase.State(self.q_name, 't_0', 0, 0,0))
        self.get_sorted_tables_mock(self.ro_cursor).returns(tuple())

        self.mc.replay()
        # Zero items behind, zero seconds behind.
        self.assertEquals((0, 0), self.mqs.read_stat())
        self.mc.verify()


if __name__ == "__main__":
    unittest2.main()
