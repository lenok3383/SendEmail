"""PHLog distributed reader class tests.

:Status: $Id: //prod/main/_is/shared/python/phonehome/test/test_distributedreader.py#6 $
:Author: vburenin
"""

import unittest2
import urllib2

from shared.db import dbcp
from shared.dbqueue import mcqsubscriber
from shared.phonehome.distributedreader import PHDistributedReader, PHLogChunk
from shared.testing.vmock import matchers, mockcontrol

TEST_APP_NAME = 'distributedreader_test'


class Test(unittest2.TestCase):

    def setUp(self):
        self.r_name = 'reader_name'
        self.q_name = 'queue_name'
        self.db_conf_section_rw = 'section_rw'
        self.db_conf_section_ro = 'section_ro'
        self.db_conf_file = 'file'
        self.rw_pool_mock = 'rw_pool_mock'
        self.ro_pool_mock = 'ro_pool_mock'
        self.stat_interval = 1

        self.mc = mockcontrol.MockControl()

        # Mock constructor logic.
        self.mqs_mock = \
            self.mc.mock_class(mcqsubscriber.MulticastQueueSubscriber,
                               display_name='MulticastQueueSubscriber')

        self.mqs_ctor_mock = \
            self.mc.mock_constructor(mcqsubscriber,
                                    'MulticastQueueSubscriber')

        self.db_pool_mock = self.mc.mock_class(dbcp.DBPoolManagerImpl)

        self.mc.mock_method(dbcp, 'DBPoolManager')(self.db_conf_file) \
            .returns(self.db_pool_mock)

        self.db_pool_mock.get_rw_pool(self.db_conf_section_rw) \
            .returns(self.rw_pool_mock)
        self.db_pool_mock.get_ro_pool(self.db_conf_section_ro) \
            .returns(self.ro_pool_mock)

        self.mqs_ctor_mock(self.q_name, self.r_name,
                           self.rw_pool_mock, self.ro_pool_mock) \
            .returns(self.mqs_mock)
        self.mqs_mock.init_queue_state()

    def tearDown(self):
        self.mc.tear_down()

    def construct_reader(self):
        return PHDistributedReader(self.r_name, self.db_conf_section_rw,
                                   self.db_conf_section_ro,
                                   self.q_name,
                                   db_conf_file_name=self.db_conf_file,
                                   status_refresh_secs=self.stat_interval)

    def test_next(self):
        """Test that next() method does exactly we expect"""
        def ret_data(phchunk):
            yield 'data'
            yield 'data1'

        # Init.
        chunk = ('h:p', 'path', 1, 2)
        self.mqs_mock.pop(1).returns((chunk,))
        self.mc.mock_method(PHDistributedReader, '_get_packets') \
            (matchers.is_type(PHLogChunk)).does(ret_data)

        # Test.
        self.mc.replay()
        ph_reader = self.construct_reader()
        self.assertEquals('data', ph_reader.next())
        self.assertEquals('data1', ph_reader.next())

        # Verify.
        self.mc.verify()

    def test_get_packets(self):
        """Test _get_packets() join data byte three received elements."""
        # Init.

        chunk = ('h:p', 'path', 1, 2)
        req = urllib2.Request(url='http://h:p/path')
        req.add_header('Host', 'h:p')
        req.add_header('Range', 'bytes=1-2')

        self.mc.mock_method(PHDistributedReader, '_build_request')(chunk) \
            .returns(req)
        self.mc.mock_method(urllib2, 'urlopen')(req) \
            .returns([str(i) for i in range(10)])

        # Test.
        self.mc.replay()
        ph_reader = self.construct_reader()
        self.assertEqual(['012', '345', '678'],
                         list(ph_reader._get_packets(chunk)))

        # Verify.
        self.mc.verify()

    def test_get_chunk_meta(self):
        """Test chunk is built correctly."""

        # Init.
        chunk = ('h:p', 'path', 1, 5)
        self.mqs_mock.pop(1).returns((chunk,))

        # Test.
        self.mc.replay()
        ph_reader = self.construct_reader()
        phlog_chunk = ph_reader._get_chunk_meta()
        self.assertEqual(phlog_chunk.hostport, chunk[0])
        self.assertEqual(phlog_chunk.file_path, chunk[1])
        self.assertEqual(phlog_chunk.start_b, chunk[2])
        self.assertEqual(phlog_chunk.end_b, chunk[3])
        self.assertEqual(4, len(phlog_chunk))

        # Verify.
        self.mc.verify()

    def test_get_chunk_meta_none(self):
        """Test there are no chunk data"""
        # Init.
        self.mqs_mock.pop(1).returns(None)

        # Test.
        self.mc.replay()
        ph_reader = self.construct_reader()
        self.assertEqual(None, ph_reader._get_chunk_meta())

        # Verify.
        self.mc.verify()

    def test_seek(self):
        """Test seek() calls an appropriate method."""
        # Init.
        self.mqs_mock.seek(100)

        # Test.
        self.mc.replay()
        self.construct_reader().seek(100)

        # Verify.
        self.mc.verify()

    def test_seek_to_start(self):
        """Test seek_to_start() calls an appropriate method."""
        # Init.
        self.mqs_mock.seek(0)

        # Test.
        self.mc.replay()
        self.construct_reader().seek(0)

        # Verify.
        self.mc.verify()

    def test_seek_to_end(self):
        """Test seek_to_end() calls an appropriate method."""
        # Init.
        self.mqs_mock.seek(-1)

        # Test.
        self.mc.replay()
        self.construct_reader().seek(-1)

        # Verify.
        self.mc.verify()

    def test_seek_to_ts(self):
        """Test seek_to_ts() calls an appropriate method."""
        # Init.
        self.mqs_mock.seek_to_ts(1000)

        # Test.
        self.mc.replay()
        self.construct_reader().seek_to_ts(1000)

        # Verify.
        self.mc.verify()

    def test_tell(self):
        """Test tell() calls an appropriate tell() method."""
        # Init.
        self.mqs_mock.tell().returns('something')

        # Test.
        self.mc.replay()
        self.assertEqual('something', self.construct_reader().tell())

        # Verify.
        self.mc.verify()

    def test_items_and_seconds_behind(self):
        """Test read stats are working well and cached"""

        # Init.
        self.mqs_mock.read_stat() \
            .returns((1, 2))

        # Test.
        self.mc.replay()
        ph_reader = self.construct_reader()
        self.assertEqual(1, ph_reader.items_behind)
        self.assertEqual(2, ph_reader.seconds_behind)

        # Verify.
        self.mc.verify()


if __name__ == "__main__":
    unittest2.main()
