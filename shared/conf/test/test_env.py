"""Unit tests for shared.conf.env module.

:Status: $Id: //prod/main/_is/shared/python/conf/test/test_env.py#5 $
:Authors: lromanov

"""

import os
import unittest2 as unittest

import shared.conf.env


class TestEnv(unittest.TestCase):

    CONFROOT = '/usr/local/ironport/prod/conf'
    PRODROOT = '/usr/local/ironport/prod'

    def setUp(self):
        # Save PRODROOT, CONFROOT, so we can mess with it.
        for var in ('PRODROOT', 'CONFROOT'):
            old = os.environ.get(var)
            if old is not None:
                self.addCleanup(os.environ.__setitem__, var, old)

    def test_get_conf_root(self):
        os.environ['CONFROOT'] = TestEnv.CONFROOT
        self.assertEquals(shared.conf.env.get_conf_root(), TestEnv.CONFROOT)

    def test_missing_conf_path(self):
        del os.environ['CONFROOT']
        self.assertRaises(
            shared.conf.env.ConfRootError,
            shared.conf.env.get_conf_root)

    def test_get_prod_root(self):
        os.environ['PRODROOT'] = TestEnv.PRODROOT
        self.assertEquals(shared.conf.env.get_prod_root(), TestEnv.PRODROOT)

    def test_missing_prod_path(self):
        del os.environ['PRODROOT']
        self.assertRaises(
            shared.conf.env.ProdRootError,
            shared.conf.env.get_prod_root)


if __name__ == '__main__':
    unittest.main()
