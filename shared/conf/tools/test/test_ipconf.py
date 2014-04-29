"""Unit tests for ipconf.

:Author: duncan
"""
import cStringIO
import logging
import socket
import unittest2 as unittest

import shared.conf.env
import shared.testing.case
import shared.testing.vmock.mockcontrol
from shared.conf.tools import ipconf

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)


class MockOptions(object):
    pass


P4_ROOT = ipconf.P4_TREE_ROOT

class TestIpconf(shared.testing.case.TestCase):

    def test_guess_package_from_confroot_trailing_slash(self):
        """Package can be determined from confroot with trailing slash."""
        self.assertEqual(
            ipconf._guess_package_from_confroot(
                '/usr/local/ironport/example/etc/'),
            'example')

    def test_guess_package_from_confroot_no_trailing_slash(self):
        """Package can be determined from confroot without trailing slash."""
        self.assertEqual(
            ipconf._guess_package_from_confroot(
                '/usr/local/ironport/example/etc'),
            'example')

    def test_guess_package_from_confroot_non_standard(self):
        """Package can be determined from non-standard confroot."""
        self.assertEqual(
            ipconf._guess_package_from_confroot(
                '/data/var/ironport/example/etc/'),
            'example')

    def test_guess_package_from_confroot_indecipherable(self):
        """Package can be determined from non-standard confroot."""
        self.assertRaises(ipconf.InvalidOptionsError,
                          ipconf._guess_package_from_confroot,
                          '/something/that/doesnt/make/sense/')

    def test_get_perforce_paths_bad_target(self):
        """No perforce paths are found with a bad env name."""
        m = MockOptions()
        m.hostname = 'foo'
        m.package = 'bar'
        self.assertEqual(
            ipconf.get_perforce_paths('doesntmake-sense', m), [])

    def test_get_perforce_paths(self):
        """Correct Perforce paths are checked."""
        m = MockOptions()
        m.hostname = 'blade99.soma.ironport.com'
        m.package = 'corpush'
        self.assertEqual(
            ipconf.get_perforce_paths('prod-whiskey-app', m),
            [P4_ROOT + '/host/blade99.soma.ironport.com/corpush.cf',
             P4_ROOT + '/env/prod/whiskey/app/corpush.cf'])

    def test_get_perforce_paths_using_confroot(self):
        """Correct Perforce paths are checked."""
        m = MockOptions()
        m.hostname = 'blade99.soma.ironport.com'
        m.confroot = '/usr/local/ironport/corpush/etc/'
        m.package = None
        self.assertEqual(
            ipconf.get_perforce_paths('prod-whiskey-app', m),
            [P4_ROOT + '/host/blade99.soma.ironport.com/corpush.cf',
             P4_ROOT + '/env/prod/whiskey/app/corpush.cf'])

    def test_get_perforce_paths_perforce_path(self):
        """Correct Perforce paths are checked."""
        m = MockOptions()
        m.hostname = 'foo'
        m.package = 'bar'
        self.assertEqual(
            ipconf.get_perforce_paths(P4_ROOT + '/some-config', m),
            [P4_ROOT + '/some-config'])


class TestOptionParser(shared.testing.case.TestCase):

    def setUp(self):
        self.mc = shared.testing.vmock.mockcontrol.MockControl()
        self.addCleanup(self.mc.tear_down)

        self.confroot = '/my/confroot/for/package/whiskey/etc'
        self.hostname = 'fake-host.soma.ironport.com'

        self.mc.stub_method(shared.conf.env, 'get_conf_root')().returns(
            self.confroot)
        self.mc.stub_method(socket, 'getfqdn')().returns(
            self.hostname)

        self.op = ipconf.get_option_parser()

    def test_defaults(self):
        """Option parser defaults are good."""

        options, args = self.op.parse_args([])
        self.assertEqual(args, [])
        self.assertFalse(options.verbose)
        self.assertEqual(options.confroot, self.confroot)
        self.assertIsNone(options.package)

    def test_verbose_short(self):
        options, args = self.op.parse_args(['-v'])
        self.assertTrue(options.verbose)

    def test_verbose_long(self):
        options, args = self.op.parse_args(['--verbose'])
        self.assertTrue(options.verbose)

    def test_confroot_short(self):
        options, args = self.op.parse_args(['-c', '/some/confroot'])
        self.assertEqual(options.confroot, '/some/confroot')

    def test_confroot_long(self):
        options, args = self.op.parse_args(['--confroot', '/some/confroot'])
        self.assertEqual(options.confroot, '/some/confroot')

    def test_package_long(self):
        options, args = self.op.parse_args(['--package', 'example'])
        self.assertEqual(options.package, 'example')

    def test_hostname_long(self):
        options, args = self.op.parse_args(['--hostname', 'example-host'])
        self.assertEqual(options.hostname, 'example-host')

    def test_args(self):
        fh = cStringIO.StringIO()
        new_op = ipconf.get_option_parser(epilog='Hello world')
        new_op.print_help(file=fh)
        self.assertIn('Hello world', fh.getvalue())


if __name__ == '__main__':
    unittest.main()
