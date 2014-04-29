"""Unit tests for ipconf_sync.

:Author: duncan
"""
import logging
import os
import unittest2 as unittest

import shared.testing.case
from shared.conf.tools import ipconf
from shared.conf.tools import ipconf_sync

import test_ipconf

CONFIGVARS_CONF = \
"""[test.awesome.var]
help=Super awesome fun variable
type=int
default=42

[test.awesome.default]
help=Nobody ever changes the default
type=str
default=hi
"""

CACHE_FILE = \
"""# Cache file
%s=0ab6f0c55870fc88c521b8797aa83eed
test.awesome.var=-42
;test.default.val=hi
""" % (ipconf.CONF_VERSION_CHECKSUM_VAR,)

DIFFERENT_CONFIGVARS_CONF = CONFIGVARS_CONF + \
"""
[test.awesome.new_var]
help=New!
type=str
default=new var
"""

INCOMPATIBLE_CONFIGVARS_CONF = CONFIGVARS_CONF + \
"""
[test.awesome.required_var]
help=Required var
type=bool
"""

P4_ROOT = ipconf.P4_TREE_ROOT

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)


class MockPerforce(object):

    RETURN_DATA = 'hello world'

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def get(self, target):
        return self.RETURN_DATA


class TestIpconfSync(shared.testing.case.TestCase):

    def _test_perforce_paths_revision(self, provided_revision,
                                      expected_revision=None):
        m = test_ipconf.MockOptions()
        m.package = 'corpush'
        m.hostname = 'blade99.soma.ironport.com'
        m.revision = provided_revision
        if expected_revision is None:
            expected_revision = provided_revision
        self.assertEqual(
            ipconf_sync._get_perforce_paths('prod-whiskey-app', m),
            [P4_ROOT + '/host/blade99.soma.ironport.com/corpush.cf' +
             expected_revision,
             P4_ROOT + '/env/prod/whiskey/app/corpush.cf' + expected_revision])

    def test_get_perforce_paths_revision_num(self):
        """Correct Perforce paths are checked when revision 123456 given."""
        self._test_perforce_paths_revision('123456', '@123456')

    def test_get_perforce_paths_revision_at_num(self):
        """Correct Perforce paths are checked when revision @123456 given."""
        self._test_perforce_paths_revision('@123456')

    def test_get_perforce_paths_revision_shelved_change(self):
        """Correct Perforce paths are checked when revision @=123456 given."""
        self._test_perforce_paths_revision('@=123456')

    def test_get_perforce_paths_revision_hash_num(self):
        """Correct Perforce paths are checked when revision #42 given."""
        self._test_perforce_paths_revision('#42')

    def test_get_perforce_paths_no_revision(self):
        """Correct Perforce paths are checked when no revision given."""
        self._test_perforce_paths_revision(None, '')

    def test_get_perforce_paths_actual_perforce_path(self):
        """Provided perforce path is used."""
        m = test_ipconf.MockOptions()
        m.revision = None
        self.assertEqual(
            ipconf_sync._get_perforce_paths('//foo/bar/baz', m),
            ['//foo/bar/baz'])

    def test_get_perforce_paths_actual_perforce_path_with_rev(self):
        """Provided perforce path is used, and provided revision is used."""
        # This use case is kinda silly, since we can specify the revision as
        # part of the perforce path.
        m = test_ipconf.MockOptions()
        m.revision = '123456'
        self.assertEqual(
            ipconf_sync._get_perforce_paths('//foo/bar/baz', m),
            ['//foo/bar/baz@123456'])

    def test_get_perforce_data(self):
        """Perforce data is read properly."""
        self.addCleanup(setattr, ipconf_sync.shared.scm.perforce, 'Perforce',
                        ipconf_sync.shared.scm.perforce.Perforce)
        ipconf_sync.shared.scm.perforce.Perforce = MockPerforce

        self.assertEqual(ipconf_sync.get_perforce_data('//user/duncan/blah'),
                         'hello world')


class TestIpconfSyncBuildFiles(shared.testing.case.TestCase):

    def setUp(self):
        self._confroot = self.get_temp_dir()
        self._configvars = os.path.join(self._confroot, '_configvars.conf')

    def _write_configvars_conf(self, text):
        with open(self._configvars, 'w') as f:
            f.write(text)

    def test_normal(self):
        """Configfiles can be properly generated."""
        self._write_configvars_conf(CONFIGVARS_CONF)
        self.assertEqual(
            ipconf_sync.build_config_files(self._confroot, CACHE_FILE,
                                           force=False),
            0)

        test_conf = os.path.join(self._confroot, 'test.conf')
        self.assertTrue(os.path.exists(test_conf))
        with open(test_conf, 'r') as f:
            data = f.read()
        self.assertIn('[awesome]\nvar = eval(-42)\ndefault = hi\n', data)

    def test_checksum_mismatch(self):
        """Config files not generated if checksum mismatched."""
        self._write_configvars_conf(DIFFERENT_CONFIGVARS_CONF)

        self.assertEqual(
            ipconf_sync.build_config_files(self._confroot, CACHE_FILE,
                                           force=False),
            1)

        test_conf = os.path.join(self._confroot, 'test.conf')
        self.assertFalse(os.path.exists(test_conf))

    def test_force_success(self):
        """Config files generated if checksum mismatched with force."""
        self._write_configvars_conf(DIFFERENT_CONFIGVARS_CONF)

        self.assertEqual(
            ipconf_sync.build_config_files(self._confroot, CACHE_FILE,
                                           force=True),
            0)

        test_conf = os.path.join(self._confroot, 'test.conf')
        self.assertTrue(os.path.exists(test_conf))
        with open(test_conf, 'r') as f:
            data = f.read()
        self.assertIn('[awesome]\nvar = eval(-42)\n', data)

    def test_force_fail(self):
        """Config files not generated with force if values incompatible."""
        self._write_configvars_conf(INCOMPATIBLE_CONFIGVARS_CONF)
        self.assertEqual(
            ipconf_sync.build_config_files(self._confroot, CACHE_FILE,
                                           force=True),
            1)

    def test_invalid_cache_missing_version(self):
        """Error given with invalid cache file (missing version)."""
        self._write_configvars_conf(CONFIGVARS_CONF)
        self.assertEqual(
            ipconf_sync.build_config_files(self._confroot, 'key=value\n',
                                           force=False),
            1)

    def test_invalid_cache_structure(self):
        """Error given with invalid cache file (structurally wrong)."""
        self._write_configvars_conf(CONFIGVARS_CONF)
        self.assertEqual(
            ipconf_sync.build_config_files(self._confroot, 'key+value\n',
                                           force=False),
            1)


if __name__ == '__main__':
    unittest.main()
