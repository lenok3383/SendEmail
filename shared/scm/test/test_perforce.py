"""Unit tests for shared.scm.perforce module.

:Status: $Id: //prod/main/_is/shared/python/scm/test/test_perforce.py#5 $
:Author: ivlesnik
:Last Modified By: $Author: jacchen2 $
"""

import unittest2

from shared.scm.perforce import Perforce, PerforceException


class TestPerforce(unittest2.TestCase):
    """Test Perforce module."""

    def setUp(self):
        """Setup a new Perforce object for each unit test."""

        self._user = 'p4'
        self._password = ''
        self._host = 'perforce.ironport.com'
        self._port = 1666

        # Paths to repository files.
        self.test_source_file = '//prod/main/_is/products/feeds_sql/cbl.sql'
        self.test_source_dir = '//prod/main/_is/products/feeds/db/...'

        # Instantiate Perforce class.
        self._perforce = Perforce(self._user, self._password, self._host,
            self._port)

    def test_perforce_files(self):
        """Test the Perforce files() method."""

        # If a directory, check that it can call files; otherwise,
        # do a p4 print on the file.
        with self._perforce:
            data = self._perforce.files(self.test_source_dir)

            msg = 'Expected data from Perforce.files().\nAttempted to '\
                'print path: %s using user: %s' % (self.test_source_dir,
                self._user)
            self.assertTrue(data, msg)

    def test_perforce_get(self):
        """Test the Perforce get() method."""

        with self._perforce:
            data = self._perforce.get(self.test_source_file)

            msg = 'Expected data from Perforce.get().\nAttempted to '\
                'print path: %s using user: %s' % (self.test_source_file,
                self._user)
            self.assertTrue(data, msg)

    def test_perforce_changes_default(self):
        """Test the Perforce changes() method."""

        for p4_path in (self.test_source_dir, self.test_source_file):
            with self._perforce:
                data = self._perforce.changes(p4_path)

            msg = 'Expected data from Perforce.changes().\nAttempted to print '\
                'path: %s with no maximum results.' % (p4_path,)
            self.assertTrue(data, msg)

    def test_perforce_changes_max_results(self):
        """Test the Perforce changes() method."""

        for p4_path in (self.test_source_dir, self.test_source_file):
            with self._perforce:
                data = self._perforce.changes(p4_path, max_results=3)

            msg = 'Expected data from Perforce.changes().\nAttempted to print '\
                'path: %s with 3 max_results.' % (p4_path,)
            self.assertTrue(data, msg)

    def test_perforce_changes_long_description(self):
        """Test the Perforce changes() method with long description output."""

        p4_path = '//prod/main/_is/shared/python/scm/perforce.py#1'
        description = '\n\tPorted common module p4_utils to python 2.6\n\t\n\tReviewer:                   duncan, vburenin\n\tReviewboard URL:            http://review.eng.ironport.com/r/26208/\n\n'
        with self._perforce:
            data = self._perforce.changes(p4_path, max_results=1)

        self.assertEqual(len(data), 1)
        self.assertEqual(len(data[0]), 4)
        self.assertEqual(data[0]['change_number'], 436896)
        self.assertEqual(data[0]['change_date'], 1313391600)
        self.assertEqual(data[0]['user'], 'ivlesnik@ivlesnik:dev-avc-vm3.vega.ironport.com:shared')
        self.assertEqual(data[0]['description'], description)

    def test_perforce_raises_exception(self):
        """Test the Perforce methods raise PerforceException."""

        invalid_p4_dir = '//wrong/repository/path/...'
        invalid_p4_file = '//wrong/repository/file.txt'
        with self._perforce:
            self.assertRaises(PerforceException, self._perforce.files,
                invalid_p4_dir)
            self.assertRaises(PerforceException, self._perforce.get,
                invalid_p4_file)
            self.assertRaises(PerforceException, self._perforce.changes,
                invalid_p4_file)

    def test_perforce_with_enter_returns_perforce(self):
        """Test that the __enter__ method returns self."""
        with self._perforce as p:
            self.assertIs(p, self._perforce)

    def test_perforce_client_output(self):
        """Test the Perforce client() method."""
        with self._perforce:
            output = self._perforce.client()

            msg = '# A Perforce Client Specification.'
            self.assertTrue(output.startswith(msg), 'Expected: ' + msg)

    def test_perforce_depot_path(self):
        """Test the Perforce depot_path() method."""
        with self._perforce:
            output = self._perforce.depot_path()

            msg = '//'
            self.assertTrue(output.startswith(msg),
                'Expected depot path to start with:' + msg + ' in output:' + output)

if __name__ == '__main__':
    unittest2.main()
