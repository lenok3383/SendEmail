"""Mass DNS querier test.

:Status: $Id: //prod/main/_is/shared/python/net/dns/test/test_mass_dns.py#3 $
:Authors: vburenin
"""

import __builtin__
import adns
import time
import unittest2

from shared.net.dns import mass_dns
from shared.testing.vmock import mockcontrol

TEST_APP_NAME = 'MASS_DNS_RESOLVER'


class FakeADNS_State(object):

    def submit(self, name, opt):
        pass


class FakeADNS_Client(object):

    def check(self):
        pass

    def cancel(self):
        pass


class MassDNSResolverTest(unittest2.TestCase):

    def setUp(self):
        self.mc = mockcontrol.MockControl()
        self.adns_init_mock = self.mc.mock_method(adns, 'init')
        self.resolver_mock = self.mc.mock_class(FakeADNS_State, 'resolver')
        self.adns_client_mock = self.mc.mock_class(FakeADNS_Client, 'client')
        self.open_mock = self.mc.mock_method(__builtin__, 'open')
        self.open_mock('/dev/null', 'w+').returns('fh')

    def tearDown(self):
        self.mc.tear_down()

    def test_constructor_use_system_resolv_conf(self):

        # Setup.
        self.adns_init_mock(0, 'fh').returns(self.resolver_mock)

        # Run.
        self.mc.replay()
        mass_dns.MassQuery(30)

        # Verify.
        self.mc.verify()

    def test_constructor_use_one_custom_dns(self):

        # Setup.
        self.adns_init_mock(0, 'fh', 'nameserver mydns.com\n') \
            .returns(self.resolver_mock)

        # Run.
        self.mc.replay()
        mass_dns.MassQuery(30, 'mydns.com')

        # Verify.
        self.mc.verify()

    def test_constructor_use_two_custom_dns(self):

        # Setup.
        self.adns_init_mock(0, 'fh', 'nameserver ns1\nnameserver ns2\n') \
            .returns(self.resolver_mock)

        # Run.
        self.mc.replay()
        mass_dns.MassQuery(30, ['ns1', 'ns2'])

        # Verify.
        self.mc.verify()

    def test_resolve_one_element(self):

        # Setup.
        self.adns_init_mock(0, 'fh').returns(self.resolver_mock)
        self.resolver_mock.submit('u.n', adns.rr.A) \
            .returns(self.adns_client_mock)
        self.adns_client_mock.check().returns('r')

        # Run.
        self.mc.replay()
        mdns = mass_dns.MassQuery(30)
        self.assertEqual([('u.n', 'r')], list(mdns.resolve(['u.n'], 'A')))
        self.mc.tear_down()

        # Verify.
        self.mc.verify()

    def test_resolve_one_element_not_ready_error(self):

        # Setup.
        self.adns_init_mock(0, 'fh').returns(self.resolver_mock)
        self.resolver_mock.submit('u.n', adns.rr.A) \
            .returns(self.adns_client_mock)
        # First attempt to get result will return NotReady error.
        self.adns_client_mock.check().raises(adns.NotReady())
        self.adns_client_mock.check().returns('r')

        # Run.
        self.mc.replay()
        mdns = mass_dns.MassQuery(30)

        self.assertEqual([('u.n', 'r')], list(mdns.resolve(['u.n'], 'A')))
        self.mc.tear_down()

        # Verify.
        self.mc.verify()

    def test_resolve_one_element_timeout(self):

        # Setup.
        time_mock = self.mc.mock_method(time, 'time')

        self.adns_init_mock(0, 'fh').returns(self.resolver_mock)
        self.resolver_mock.submit('u.n', adns.rr.A) \
            .returns(self.adns_client_mock)
        time_mock().returns(1)
        # First attempt to get result will return NotReady error.
        self.adns_client_mock.check().raises(adns.NotReady())
        # Increase time significantly to cause TimeoutError.
        time_mock().returns(100)
        self.adns_client_mock.cancel()

        # Run.
        self.mc.replay()
        mdns = mass_dns.MassQuery(30)
        res = list(mdns.resolve(['u.n'], 'A'))
        self.assertEqual('u.n', res[0][0])
        self.assertTrue(isinstance(res[0][1], mass_dns.TimeoutError))
        self.mc.tear_down()

        # Verify.
        self.mc.verify()


if __name__ == '__main__':
    unittest2.main()
