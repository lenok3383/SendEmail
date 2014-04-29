"""MulticastQueuePublisher class tests.

:Status: $Id: //prod/main/_is/shared/python/dbqueue/test/test_mcqpublisher.py#8 $
:Author: vburenin
"""

import time
import threading
import unittest2
from array import array
from datetime import datetime
from MySQLdb import ProgrammingError

from shared.dbqueue.errors import NoSuchTableError
from shared.dbqueue.mcqpublisher import MulticastQueuePublisher
from shared.dbqueue.test import mockobjects
from shared.testing.vmock import matchers
from shared.testing.vmock import mockcontrol
from shared.dbqueue import mcqbase


TEST_APP_NAME = 'mcqpublisher_test'

class Test(unittest2.TestCase):

    def setUp(self):
        self.q_name = 't'
        self.p_name = 'p'
        self.mc = mockcontrol.MockControl()
        self.pool_mock = mockobjects.ConnectionPoolMock(self.mc)
        self.cursor = self.pool_mock.cursor_mock
        self.time_mock = self.mc.mock_method(time, 'time')
        self.rlock_mock = None
        self.mqp = None
        self.db_state_mock = None
        self.update_queue_mock = None
        self.get_first_ts_mock = None
        self.mc.mock_method(self, 'call_back_func')

    def tearDown(self):
        self.mc.tear_down()

    def call_back_func(self, *args, **kwargs):
        pass

    def init_publisher(self, set_callback, rollover_seconds, auto_purge,
                       purge_threshold_hours):
        if set_callback:
            purge_callback = self.call_back_func
        else:
            purge_callback = None

        self.rlock_mock = self.mc.mock_class(threading._RLock)
        self.mc.stub_method(threading, 'RLock')()\
            .returns(self.rlock_mock)

        self.mqp = MulticastQueuePublisher(self.q_name, self.pool_mock,
                                           rollover_seconds, auto_purge,
                                           purge_callback,
                                           purge_threshold_hours)
        self.db_state_mock = self.mc.mock_method(self.mqp, '_get_state_from_db')
        self.get_first_ts_mock = self.mc.mock_method(self.mqp, '_get_first_ts')
        self.update_queue_mock = self.mc.mock_method(self.mqp,
                                                     '_update_queue_state')

    def test_append_none(self):
        """Append should ignore None"""
        self.init_publisher(True, 300, True, 5)
        self.mqp.append(None)

    def test_append_to_new_table(self):
        """Test 'append' creates new table to add new record."""
        self.init_publisher(False, 1, True, 5)
        self.time_mock().returns(10).anyorder()

        self.db_state_mock(self.cursor, lock=True)\
            .returns(mcqbase.State(self.q_name, 't_2', 1, 4,
                                   datetime(2011, 10, 11)))

        self.rlock_mock.acquire()
        self.get_first_ts_mock(self.cursor, 't_2').raises(NoSuchTableError())
        self.rlock_mock.release()
        self.cursor.execute(matchers.str_with('CREATE TABLE'))
        self.cursor.executemany(matchers.str_with('INTO t_3'),
                            ["S'aa'\np1\n.", "S'bb'\np1\n."])
        self.cursor.lastrowid = 5
        self.cursor.rowcount = 1
        self.update_queue_mock(self.cursor, 5, 't_3', 6)

        self.mc.replay()
        self.mqp.append(['aa', 'bb'])
        self.mc.verify()

    def test_append_to_existing_table_cached(self):
        """Should be no additional calls to get table time stamp."""
        self.init_publisher(True, 300, True, 5)
        self.time_mock().returns(10).anyorder()
        self.mqp._table_name_to_ts = {'t_2': 2}

        self.db_state_mock(self.cursor, lock=True)\
            .returns(mcqbase.State(self.q_name, 't_2', 1, 4,
                                   datetime(2011, 10, 11)))
        self.rlock_mock.acquire()
        self.rlock_mock.release()
        self.cursor.executemany(matchers.str_with('INTO t_2'),
                            ["S'aa'\np1\n.", "S'bb'\np1\n."])
        self.cursor.lastrowid = 5
        self.cursor.rowcount = 1

        self.update_queue_mock(self.cursor, 5, 't_2', 6)

        self.mc.replay()
        self.mqp.append(['aa', 'bb'])
        self.mc.verify()

    def test_append_to_existing_table_non_cached(self):
        """Expect get_first_ts_mock will be called to get table time stamp."""
        self.init_publisher(True, 300, True, 5)
        self.time_mock().returns(10).anyorder()
        self.mqp._table_name_to_ts = {}

        self.db_state_mock(self.cursor, lock=True)\
            .returns(mcqbase.State(self.q_name, 't_2', 1, 4,
                                   datetime(2011, 10, 11)))

        self.rlock_mock.acquire()
        self.get_first_ts_mock(self.cursor, 't_2').returns(2)
        self.rlock_mock.release()

        self.cursor.executemany(matchers.str_with('INTO t_2'),
                            ["S'aa'\np1\n.", "S'bb'\np1\n."])
        self.cursor.lastrowid = 5
        self.cursor.rowcount = 1

        self.update_queue_mock(self.cursor, 5, 't_2', 6)

        self.mc.replay()
        self.mqp.append(['aa', 'bb'])
        self.mc.verify()

    def test_purge_not_yet_with_callback(self):
        """Purge is called but it is not time to do that"""
        self.init_publisher(True, 300, True, 5)
        self.time_mock().returns(10).anyorder()

        # Execute.
        self.mc.replay()
        self.mqp.purge('a', b='b')
        self.mc.verify()

    def test_purge_clean_tables(self):
        """Purge is called and it is time to do that"""
        # Change time to a few thousands years ahead.
        self.init_publisher(False, 300, True, 5)
        self.time_mock().returns(2614635751).anyorder()
        last_ts_mock = self.mc.mock_method(self.mqp, '_get_last_ts')

        self.db_state_mock(self.cursor, lock=True).returns(
                    mcqbase.State(self.q_name, 't_2', 1, 4,
                                  datetime(2011, 10, 11)))

        self.mc.mock_method(self.mqp, '_get_sorted_data_tables')(self.cursor) \
            .returns(('t_1', 't_2'))

        last_ts_mock(self.cursor, 't_1').returns(10)
        self.cursor.execute('DROP TABLE IF EXISTS t_1')

        self.rlock_mock.acquire()
        self.rlock_mock.release()

        # Execute.
        self.mc.replay()
        self.mqp.purge('a', b='b')
        self.mc.verify()

    def test_purge_interrup_by_callback(self):
        """Test interruption of the purge using callback."""
        self.init_publisher(True, 300, True, 5)
        self.time_mock().returns(2614635751).anyorder()
        last_ts_mock = self.mc.mock_method(self.mqp, '_get_last_ts')
        self.db_state_mock(self.cursor, lock=True).returns(
                    mcqbase.State(self.q_name, 't_4', 1, 4,
                                  datetime(2011, 10, 11)))

        self.mc.mock_method(self.mqp, '_get_sorted_data_tables')(self.cursor) \
            .returns(('t_1', 't_2', 't_3', 't_4'))

        last_ts_mock(self.cursor, 't_1').returns(10)
        self.cursor.execute('SELECT data FROM t_1')
        self.cursor.fetchall().returns([("S'a1'\np1\n.",), ("S'a2'\np1\n.",)])
        self.call_back_func(['a1', 'a2']).returns(True)
        self.cursor.execute('DROP TABLE IF EXISTS t_1')

        self.rlock_mock.acquire()
        self.rlock_mock.release()

        # Test processing of arrays.
        item1_mock = self.mc.mock_obj(array('c'))
        item2_mock = self.mc.mock_obj(array('c'))
        last_ts_mock(self.cursor, 't_2').returns(10)
        self.cursor.execute('SELECT data FROM t_2')
        self.cursor.fetchall().returns([(item1_mock,), (item2_mock,)])
        item1_mock.tostring().returns("S'b1'\np1\n.")
        item2_mock.tostring().returns("S'b2'\np1\n.")
        self.call_back_func(['b1', 'b2']).returns(True)
        self.cursor.execute('DROP TABLE IF EXISTS t_2')

        self.rlock_mock.acquire()
        self.rlock_mock.release()

        # Interruption by callback.
        last_ts_mock(self.cursor, 't_3').returns(10)
        self.cursor.execute('SELECT data FROM t_3')
        self.cursor.fetchall().returns([("S'b1'\np1\n.",), ("S'b2'\np1\n.",)])
        self.call_back_func(['b1', 'b2']).returns(False)

        # Execute.
        self.mc.replay()
        self.mqp.purge()
        self.mc.verify()

    def test_purge_error_handling(self):
        """Test error handling in the purge method."""
        self.init_publisher(True, 300, True, 5)
        self.time_mock().returns(2614635751).anyorder()
        last_ts_mock = self.mc.mock_method(self.mqp, '_get_last_ts')
        self.db_state_mock(self.cursor, lock=True).returns(
                    mcqbase.State(self.q_name, 't_3', 1, 4,
                                  datetime(2011, 10, 11)))

        self.mc.mock_method(self.mqp, '_get_sorted_data_tables')(self.cursor) \
            .returns(('t_0', 't_1', 't_2', 't_3'))

        last_ts_mock(self.cursor, 't_0').returns(10)
        self.cursor.execute('SELECT data FROM t_0').raises(ProgrammingError())
        self.call_back_func([]).returns(True)
        self.cursor.execute('DROP TABLE IF EXISTS t_0')

        self.rlock_mock.acquire()
        self.rlock_mock.release()


        last_ts_mock(self.cursor, 't_1').returns(10)
        self.cursor.execute('SELECT data FROM t_1')
        self.cursor.fetchall().returns([("S'a1'\np1\n.",), ("S'a2'\np1\n.",)])
        self.call_back_func(['a1', 'a2']).returns(True)
        self.cursor.execute('DROP TABLE IF EXISTS t_1')

        self.rlock_mock.acquire()
        self.rlock_mock.release()

        last_ts_mock(self.cursor, 't_2').returns(10)
        self.cursor.execute('SELECT data FROM t_2')
        self.cursor.fetchall().returns([("S'b1'\np1\n.",), ("b2",)])
        self.call_back_func(['b1']).returns(True)
        self.cursor.execute('DROP TABLE IF EXISTS t_2')

        self.rlock_mock.acquire()
        self.rlock_mock.release()

        # Execute.
        self.mc.replay()
        self.mqp.purge()
        self.mc.verify()

    def test_purge_no_table_error_handling(self):
        """Test NoSuchTableError handling in the purge method."""
        self.init_publisher(True, 300, True, 5)
        self.time_mock().returns(2614635751).anyorder()
        last_ts_mock = self.mc.mock_method(self.mqp, '_get_last_ts')
        self.db_state_mock(self.cursor, lock=True).returns(
                    mcqbase.State(self.q_name, 't_3', 1, 4,
                                  datetime(2011, 10, 11)))

        self.mc.mock_method(self.mqp, '_get_sorted_data_tables')(self.cursor) \
            .returns(('t_0', 't_1', 't_2', 't_3'))

        last_ts_mock(self.cursor, 't_0').raises(NoSuchTableError())

        # Execute.
        self.mc.replay()
        self.mqp.purge()
        self.mc.verify()

    def test_init_queue_state_no_tables(self):
        """Init queue state, nothing in the queue_state table"""
        self.init_publisher(False, 300, True, 5)
        self.mc.mock_method(self.mqp, '_get_sorted_data_tables')\
            (self.cursor).returns(tuple())
        self.mc.mock_method(self.mqp, '_insert_queue_state')\
            (self.cursor, 't_0')

        self.mc.replay()
        self.mqp.init_queue_state()
        self.mc.verify()

    def test_insert_queue_state_with_table(self):
        """Init queue state, info exists in the queue_state table"""
        self.init_publisher(False, 300, True, 5)

        self.mc.mock_method(self.mqp, '_get_sorted_data_tables')\
            (self.cursor).returns(('t_1', 't_2', 't_3'))

        self.db_state_mock(self.cursor)\
            .returns(None)

        self.mc.mock_method(self.mqp, '_get_last_pos_id')(self.cursor, 't_3')\
            .returns(2)

        self.mc.mock_method(self.mqp, '_insert_queue_state')\
            (self.cursor, 't_3', 2)

        self.mc.replay()
        self.mqp.init_queue_state()
        self.mc.verify()

    def test_len(self):
        """Test queue length is taken correctly."""
        self.init_publisher(False, 300, True, 5)
        self.mc.mock_method(self.mqp, '_get_sorted_data_tables')(self.cursor)\
            .returns(('t_1', 't_2'))

        last_pos_mock = self.mc.mock_method(self.mqp, '_get_last_pos_id')
        last_pos_mock(self.cursor, 't_1').returns(2)
        last_pos_mock(self.cursor, 't_2').returns(3)

        self.mc.replay()
        self.assertEqual(5, len(self.mqp))
        self.mc.verify()

if __name__ == "__main__":
    unittest2.main()
