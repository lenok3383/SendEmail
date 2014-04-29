"""DB MulticastQueue util tests.

:Status: $Id: //prod/main/_is/shared/python/dbqueue/test/test_utils.py#3 $
:Author: vburenin
"""

import datetime
import logging
import unittest2

from shared.dbqueue import utils

TEST_APP_NAME = 'dbmcutils_test'

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)


class TestMulticastQueue(unittest2.TestCase):

    def test_parse_table_num(self):
        """Parse table name"""
        self.assertEqual(1, utils.get_table_num('t_1'))

    def test_mtime_to_ts(self):
        """Convert datetime to timestamp"""
        self.assertEqual(1318230000.0,
                         utils.mtime_to_ts(datetime.date(2011, 10, 10)))

if __name__ == "__main__":
    unittest2.main()
