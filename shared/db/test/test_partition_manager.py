"""Partition manager unit tests.

:Authors: gmazzola
:Status: $Id: //prod/main/_is/shared/python/db/test/test_partition_manager.py#1 $
"""

import contextlib
import MySQLdb.cursors
import time
import unittest2

import shared.db.test.mocks

from shared.db import dbcp
from shared.db.partition_manager import PartitionManager, LAST_PARTITION_NAME
from shared.testing.vmock import mockcontrol, matchers

SQL_GET_PARTITIONS = """SELECT partition_name
                   FROM INFORMATION_SCHEMA.partitions
                   WHERE table_name = %s AND table_schema = DATABASE()"""

RESULT_GET_PARTITIONS = [('p1322352000'), ('p1322438400'), ('p1322524800'),
                         ('p1322611200'), ('p1322697600'), ('p1322784000'),
                         ('p1322870400'), ('p1322956800'), ('p1323043200'),
                         ('p1323129600'), ('p1323216000'), ('p1323302400'),
                         (LAST_PARTITION_NAME)]

SQL_CREATE_PARTITION = """ALTER TABLE %%s REORGANIZE PARTITION %s INTO
                  (PARTITION %%s VALUES LESS THAN (%%d),
                   PARTITION %s VALUES LESS THAN (MAXVALUE))""" % \
                  (LAST_PARTITION_NAME, LAST_PARTITION_NAME)

SQL_DROP_PARTITION = """ALTER TABLE %s DROP PARTITION %s"""

PRODUCT_NAME = 'mock_product'
TABLE_NAME = 'mock_table'

CONFIG = {'mock_table.oldest_partition': '30d',
          'mock_table.partition_size': '1d',
          'mock_table.partitions_ahead': 'eval(int(10))'}

NUM_PARTITIONS = 7
NUM_OLD = 2

PARTITION_SIZE = 60 * 60 * 24
OLDEST_PARTITION = (NUM_PARTITIONS - NUM_OLD) * PARTITION_SIZE
PARTITIONS_AHEAD = 1

class PartitionManagerTest(unittest2.TestCase):

    def setUp(self):
        self.mc = mockcontrol.MockControl()

        # Disable the DBPoolManager by mocking it.
        self.rw_pool_mock = ConnectionPoolMock(self.mc)
        self.cursor_mock = self.rw_pool_mock.cursor_mock

        self.partman = PartitionManager(CONFIG, PRODUCT_NAME)

    def tearDown(self):
        self.mc.tear_down()

    def test_get_partitions(self):
        """Test the _get_partitions function."""

        # Mock the call to cursor.execute()
        self.cursor_mock.execute(SQL_GET_PARTITIONS, (TABLE_NAME,))

        # Mock the call to cursor.fetchall()
        self.cursor_mock.fetchall().returns(RESULT_GET_PARTITIONS)

        expected = [row[0] for row in RESULT_GET_PARTITIONS]

        self.mc.replay()
        result = self.partman._get_partitions(TABLE_NAME)
        self.mc.verify()

        self.assertEqual(result, expected)

    def test_create_partition(self):
        """Test the _create_partition function."""

        part_name = 'p1322352000'
        part_time = int(part_name[1:])
        sql = SQL_CREATE_PARTITION % (TABLE_NAME, part_name, part_time)

        # Mock the call to cursor.execute()
        self.cursor_mock.execute(sql)

        self.mc.replay()
        self.partman._create_partition(TABLE_NAME, part_name)
        self.mc.verify()

    def test_drop_partition(self):
        """Test the _drop_partition function."""

        partition = 'p1322352000'
        sql = 'ALTER TABLE %s DROP PARTITION %s' % (TABLE_NAME, partition)

        # Mock the call to cursor.execute()
        self.cursor_mock.execute(sql)

        self.mc.replay()
        self.partman._drop_partition(TABLE_NAME, partition)
        self.mc.verify()

    def test_merge_partitions(self):
        """Test the _merge_partitions function."""

        list = ['p1322956800', 'p1323475200', 'p1323993600', 'p1325030400',
                'p1325548800', 'p1326067200', 'p1326585600', 'p1327104000',
                'p1327622400', 'last_partition']

        partitions = ', '.join(list)
        sql = """ALTER TABLE %s REORGANIZE PARTITION %s INTO
                  (PARTITION %s VALUES LESS THAN (MAXVALUE))""" % \
                   (TABLE_NAME, partitions, LAST_PARTITION_NAME)

        self.cursor_mock.execute(sql)

        self.mc.replay()
        self.partman._merge_partitions(TABLE_NAME, list)
        self.mc.verify()

    def test_update_partitions_basic(self):
        """Test the basic case for the partition manager. This test deletes
           two old partitions and creates one partition."""

        partitions = []

        # Round ctime down to the nearest `PARTITION_SIZE' seconds, so we can mock
        # some previously-created partitions.
        ctime = int(time.time())
        ctime -= ctime % PARTITION_SIZE

        # Create some partitions, including 2 old ones.
        for pid in xrange(NUM_PARTITIONS):
            ptime = ctime - PARTITION_SIZE * pid
            partitions.append('p%s' % (ptime,))

        partitions.append(LAST_PARTITION_NAME)

        # Mock the call to PartitionManager._get_partitions()
        self.get_mock = self.mc.mock_method(self.partman, '_get_partitions')
        self.get_mock(TABLE_NAME).returns(partitions)

        # Mock the calls to PartitionManager._drop_partition()
        self.drop_mock = self.mc.mock_method(self.partman, '_drop_partition')

        for pid in xrange(NUM_PARTITIONS - NUM_OLD, NUM_PARTITIONS):
            ptime = ctime - PARTITION_SIZE * pid
            self.drop_mock(TABLE_NAME, 'p%s' % ptime)

        # Mock the call to PartitionManager._create_partition()
        self.create_mock = self.mc.mock_method(self.partman, '_create_partition')
        ptime = ctime + PARTITION_SIZE
        self.create_mock(TABLE_NAME, 'p%s' % ptime)

        self.mc.replay()
        self.partman._update_partitions(TABLE_NAME, PARTITION_SIZE,
                                        OLDEST_PARTITION, PARTITIONS_AHEAD)
        self.mc.verify()

    def test_update_partitions_unpartitioned(self):
        """Tests an error case for the partition manager, where TABLE_NAME
           has no partitions configured."""

        # Mock the call to PartitionManager._get_partitions()
        self.get_mock = self.mc.mock_method(self.partman, '_get_partitions')
        self.get_mock(TABLE_NAME).returns([])

        self.mc.replay()

        # The _update_partitions() function should raise an exception because
        # there are no partitions configured.
        with self.assertRaises(Exception):
            self.partman._update_partitions(TABLE_NAME, PARTITION_SIZE,
                                            OLDEST_PARTITION, PARTITIONS_AHEAD)

        self.mc.verify()

    def test_update_partitions_size_changed(self):
        """Tests an error case for the partition manager, where the partition_size
           used to create previously-allocated future partitions has changed."""

        # The CONFIG for these tests specifies a partition_size of 1d, but these
        # partitions are spaced 5d apart. This should trigger a failure case.
        partitions = ['first_partition', 'p1322784000', 'p1323216000',
                      'p1323648000', 'p1324080000', 'p1324512000',
                      'p1324944000', 'p1325376000', 'p1325808000',
                      'p1326240000', 'p1326672000', 'last_partition']

        # Mock time.time() so that these partitions are in the future.
        self.time_mock = self.mc.stub_method(time, 'time')
        self.time_mock().returns(0)

        # Mock the call to PartitionManager._get_partitions().
        self.get_mock = self.mc.mock_method(self.partman, '_get_partitions')
        self.get_mock(TABLE_NAME).returns(partitions)

        # Verify that the PartitionManager merges the partitions.
        self.merge_mock = self.mc.mock_method(self.partman, '_merge_partitions')
        self.merge_mock(TABLE_NAME, partitions[2:-1])

        # The PartitionManager then creates future partitions.
        self.add_mock = self.mc.mock_method(self.partman, '_create_partition')
        self.add_mock(TABLE_NAME, 'p1322870400')

        self.mc.replay()
        self.partman._update_partitions(TABLE_NAME, PARTITION_SIZE,
                                        OLDEST_PARTITION, PARTITIONS_AHEAD)
        self.mc.verify()

class ConnectionPoolMock:
    def __init__(self, mock_control):
        if (not hasattr(dbcp.pool.ConnectionPool, 'transaction')):
            raise Exception('shared.db.pool.ConnectionPool ' \
                            'interface was changed')

        self.mc = mock_control
        self.cursor_mock = self.mc.mock_class(MySQLdb.cursors.SSCursor,
                                               display_name='cursor')

        self.get_pool_mock = self.mc.stub_method(shared.db, 'get_rw_pool')
        self.get_pool_mock(PRODUCT_NAME).returns(self)

    @contextlib.contextmanager
    def transaction(self, timeout=None, cursor_class=None):
        yield self.cursor_mock

if __name__ == "__main__":
    unittest2.main()
