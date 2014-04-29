"""Unit tests for shared.file.md5 module.

:Status: $Id: //prod/main/_is/shared/python/file/test/test_md5.py#2 $
:Author: ivlesnik
:Last Modified By: $Author: mikmeyer $
"""

import getpass
import os
import unittest2

from shared.file import md5

class TestMD5Utils(unittest2.TestCase):

    """MD5 Utilities Test Suite."""

    def setUp(self):
        """Make setup for each unit test."""

        current_user = getpass.getuser()
        self.expected_md5_str = 'd41d8cd98f00b204e9800998ecf8427e'
        self.md5_file_path = '/tmp/%s_ut_md5' % (current_user,)
        self.test_file_path = '../__init__.py'
        with open(self.md5_file_path, 'w') as fp:
            fp.write(self.expected_md5_str)

    def tearDown(self):
        """Cleanup required after each test."""

        os.unlink(self.md5_file_path)

    def test_verify_md5(self):
        """Test verify_md5 function."""

        # Case 1: positive.
        try:
            md5.verify_md5(self.test_file_path, self.expected_md5_str)
        except md5.MD5MismatchError:
            self.fail('MD5 checksums comparison failed.')

        # Case 2: negative.
        invalid_file_path = './test_md5.py'
        self.assertRaises(md5.MD5MismatchError,
            md5.verify_md5, invalid_file_path, self.expected_md5_str)

    def test_check_file_md5(self):
        """Test check_file_md5 function."""

        actual_md5_str = md5.check_file_md5(self.test_file_path,
            self.md5_file_path)

        self.assertEqual(actual_md5_str, self.expected_md5_str)

    def test_compute_file_md5_hexdigest(self):
        """Test compute_md5 function with hexdigest result."""

        actual_md5_str = md5.compute_file_md5(self.test_file_path).hexdigest()
        self.assertEqual(actual_md5_str, self.expected_md5_str)


if __name__ == '__main__':

    unittest2.main()
