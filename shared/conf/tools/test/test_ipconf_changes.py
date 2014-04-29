"""Unit tests for ipconf_changes.

:Author: bwhitela
"""
import logging
import os
import unittest2 as unittest

import shared.testing.case
from shared.conf.tools import ipconf
from shared.conf.tools import ipconf_changes
from shared.conf.tools.test import test_ipconf


P4_ROOT = ipconf.P4_TREE_ROOT

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)


class MockPerforce(object):

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def changes(self, path, timeout=10, max_results=None):
        return (path, timeout, max_results)


class TestIpconfVersions(shared.testing.case.TestCase):

    def _test_get_perforce_change_data(self, path, timeout, max_results):
        """Core test for all test_get_perforce_change_data_* tests."""
        self.addCleanup(setattr, ipconf_changes.shared.scm.perforce,
                        'Perforce', ipconf_changes.shared.scm.perforce.Perforce)
        ipconf_changes.shared.scm.perforce.Perforce = MockPerforce
        options = test_ipconf.MockOptions()
        options.timeout = timeout
        options.max_results = max_results

        out = ipconf_changes.get_perforce_change_data(path, options)
        self.assertEqual(out, (path, timeout, max_results))

    def test_get_perforce_change_data0(self):
        """Test get_perforce_change_data."""
        path = '//prod/main/_is/env/test0.cf'
        timeout = 20
        max_results = 5
        self._test_get_perforce_change_data(path, timeout=timeout,
                                            max_results=max_results)

    def test_get_perforce_change_data1(self):
        """Test get_perforce_change_data."""
        path = '//prod/main/_is/env/test1.cf'
        timeout = 50
        max_results = 10
        self._test_get_perforce_change_data(path, timeout=timeout,
                                            max_results=max_results)

    def test_format_change_data_single(self):
        """Test format_change_data with a single change."""
        test_change_data = [{'user': 'user0',
                             'change_number': 123456,
                             'change_date': 12345678,
                             'description': 'Test description 0.'}]
        correct_change_str = 'User user0 submitted 123456 on 1970/05/23.\n' \
                             'Description: Test description 0.\n'
        test_change_str = ipconf_changes.format_change_data(test_change_data)
        self.assertEqual(test_change_str, correct_change_str)

    def test_format_change_data_double(self):
        """Test format_change_data with multiple changes."""
        test_change_data = [{'user': 'user1',
                             'change_number': 234567,
                             'change_date': 12345678,
                             'description': 'Test description 1.'},
                            {'user': 'user2',
                             'change_number': 345678,
                             'change_date': 23456789,
                             'description': 'Test description 2.'}]
        correct_change_str = 'User user1 submitted 234567 on 1970/05/23.\n' \
                             'Description: Test description 1.\n' \
                             '\n' \
                             'User user2 submitted 345678 on 1970/09/29.\n' \
                             'Description: Test description 2.\n'
        test_change_str = ipconf_changes.format_change_data(test_change_data)
        self.assertEqual(test_change_str, correct_change_str)


if __name__ == '__main__':
    unittest.main()
