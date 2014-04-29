#!/usr/bin/env python

"""Test the iputils module.
:Status: $Id: //prod/main/_is/shared/python/net/test/test_iputils.py#14 $
:Authors: jwescott, kylev
"""

# relies on unittest2 package
import socket
import unittest2 as unittest

from shared.net import iputils


class IputilsTestCase(unittest.TestCase):

    IP_TEST_COLUMNS = ('verdict','routable','sip','iip','cidr','test_ip','comment')
    IP_TEST_DATA = [
        (4, True,  '198.133.219.25',                  3330661145,                              32,  '198.133.219.25',                          'cisco.com'),
        (4, True,  '198.133.219.25',                  3330661145,                              32,   3330661145,                               'cisco.com'),
        (4, True,  '72.21.214.128',                   1209390720,                              32,  '72.21.214.128',                           'amazon.com'),
        (4, True,  '72.21.214.128',                   1209390720,                              32,   1209390720,                               'amazon.com'),
        (4, False, '192.168.4.101',                   3232236645,                              32,  '192.168.4.101',                           'private network'),
        (4, False, '192.168.4.101',                   3232236645,                              32,   3232236645,                               'private network'),
        (0, False, '',                                0,                                       None, 0,                                        ''),
        (4, False, '172.16.0.22',                     2886729750,                              32,  '172.16.0.22',                             'non routable'),
        (4, False, '172.31.255.255',                  2887778303,                              32,  '172.31.255.255',                          'non routable'),
        (4, False, '10.0.0.22',                       167772182,                               32,  '10.0.0.22',                               'non routable'),
        (4, True,  '198.133.219.0',                   3330661120,                              24,  '198.133.219.0/24',                        ''),
        (4, True,  '198.133.219.0',                   3330661120,                              24,  '198.133.219.3/24',                        ''),
        (0, True,  '198.133.219.3',                   3330661123,                              None,'198.133.219.3/24/9',                      ''),
        (0, True,  '198.133.219.0',                   3330661120,                              None,'198.133.219.0/64',                        ''),
        (4, True,  '222.2.0.0',                       3724673024,                              16,  '222.2.*',                                 ''),
        (4, True,  '222.2.0.0',                       3724673024,                              24,  '222.2.*/24',                              ''),
        (4, True,  '222.2.0.0',                       3724673024,                              16,  '222.2',                                   ''),
        (0, False, '266.2.0.0',                       0,                                       24,  '266.2.*/24',                              ''),
        (0, False, '266.2.0.0',                       0,                                       16,  '266.2',                                   ''),
        (4, True,  '222.2.0.0',                       3724673024,                              16,  '222.2.',                                  ''),
        (0, False, '266.277.288.299',                 0,                                       None,'266.277.288.299',                         ''),
        (6, True,  '2001:420:1101:1::a',              42540571832177911934920080078917337098,  128, '2001:420:1101:1::a',                      ''),
        (6, True,  '2001:67:ac:150::250',             42540496322684523168856853544785936976,  128, '2001:67:ac:150::250',                     ''),
        (0, True,  '2001:67:ac:150::250',             42540496322684523168856853544785936976,  None,'2001:67:ac:150::250/150',                 ''),
        (6, True,  '2001:67:ac:150::250',             42540496322684523168856853544785936976,  128, '2001:0067:00ac:0150:0000:0000:0000:0250', ''),
        (6, True,  '2001:67:ac:150::250',             42540496322684523168856853544785936976,  128, '2001:0067:00ac:0150:0000:0000::0250',     ''),
        (0, False, '2001:0067:00ac:0150::0250cc',     0,                                       None,'2001:0067:00ac:0150::0250cc',             ''),
        (0, False, '2001:ffff:exyz:0150::0250',       0,                                       None,'2001:ffff:exyz:0150::0250',               ''),
        (6, False, '2001:db8::ff00:42:8329',          42540766411282592856904265327123268393,  128, '2001:0db8:0000:0000:0000:ff00:0042:8329', 'Documentation only'),
        (0, False, 'obviously.invalid.0.0',           0,                                        16, 'obviously.invalid',                       ''),
        (6, True,  '2001:67:ac:150::250',             42540496322684523168856853544785936976,  128, '2001:67:ac:150::250/128',                 ''),
        (6, True,  '2001:67:ac:150::',                42540496322684523168856853544785936384,   64, '2001:67:ac:150::250/64',                  '/64 should ignore everything after'),
        (6, False, '::',                              0,                                       128, '::/128',                                  'the unspecified address'),
        (6, False, '::',                              0,                                         0, '::/0',                                    'default unicast route address'),
        (6, False, '::',                              0,                                       128, '::',                                      ''),
        (6, True,  'ff00::',                          338953138925153547590470800371487866880,   8, 'ff00::/8',                                'Multicast'),
        (6, False, '::1',                             1,                                       128, '::1/128',                                 'loopback'),
        (6, False, 'fe80::',                          338288524927261089654018896841347694592,  10, 'fe80::/10',                               'link-local prefix'),
        (6, False, 'fe80::4934:9627:adc4:8f14',       338288524927261089659293903002068619028, 128, 'fe80::4934:9627:adc4:8f14',               'From SBNP, local addr'),
        (6, False, 'fe80::ecc4:f6f2:e400:ed59',       338288524927261089671079929652801760601, 128, 'fe80::ecc4:f6f2:e400:ed59',               'From SBNP, local addr'),
        (6, False, 'fc00::',                          334965454937798799971759379190646833152,   7, 'fc00::/7',                                'Unique local addresses'),
        (6, True,  '64:ff9b::',                       524413980667603649783483181312245760,     96, '64:ff9b::/96',                            'the well known prefix ip4 to 6 translation'),
        (6, False, '2001:db8::',                      42540766411282592856903984951653826560,   32, '2001:db8::/32',                           'used in documentation'),
        (6, False, '::ffff:198.133.219.25',           281474012404505,                         128, '::ffff:198.133.219.25',                   'ipv4 over ipv6'),
        (6, True,  '2002::',                          42545680458834377588178886921629466624,   16, '2002::/16',                               'ipv6 over ipv4 special address'),
        (0, False, '',                                0,                                       128, '20014BA0FFF7005D0000000000000002ABCD/128','From vector'),
        (6, True,  '2001:4ba0:fff7:5d::2',            42542022098419402815561172222138646530,  128, '20014BA0FFF7005D0000000000000002/128',    'From vector'),
        (6, True,  '2400:b800:1:1:20c:29ff:fe2c:3d91',47855939811625252276308958234467384721,  128, '2400B80000010001020C29FFFE2C3D91/128',    'From vector'),
        (6, True,  '2001:13e8:2:1::87',               42540891908694433321700706457330122887,  128, '2001:13e8:2:1::87',                       'Random from SBNP'),
        (6, True,  '2001:420:4420:13::dead:132',      42540571847999124468258125418804871474,  128, '2001:420:4420:13::dead:132',              'Random from SBNP'),
        (6, True,  '2001:44b8:1a8:c002::70',          42541881944324284977552103939850109040,  128, '2001:44b8:1a8:c002::70',                  'Random from SBNP'),
        (6, True,  '2001:67c:21d0:3002:a1ef::20',     42540619691190305056088202894444068896,  128, '2001:67c:21d0:3002:a1ef::20',             'Random from SBNP'),
        (6, True,  '2607:f8c8:4:1025::236',           50552055801055667374548754550342287926,  128, '2607:f8c8:4:1025::236',                   'Random from SBNP'),
        (6, True,  '2620:0:5080:1::241',              50676817364213460963522666279242564161,  128, '2620:0:5080:1::241',                      'Random from SBNP'),
        (6, True,  '2a00:b540:0:dead::61',            55831252009708180083870158784059932769,  128, '2a00:b540:0:dead::61',                    'Random from SBNP'),
        (6, True,  '2a00:b540:0:dead::',              55831252009708180083870158784059932672,  128, '2a00:b540:0:dead::',                      'Trailing double colons'),
        ]

    def test_bin_to_ipv6long(self):
        res = iputils.bin_to_ipv6long(
            '\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x00')
        self.assertEqual(1339673755198158349044581307228491520L, res)

    def test_ipv6long_to_bin(self):
        res = iputils.ipv6long_to_bin(1339673755198158349044581307228491520L)
        self.assertEqual(
            '\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x00',
            res)

    def test_ip_to_signed_int(self):
        ips_answers = [('0.0.0.1', 1),
                       ('59.123.45.67', 997928259),
                       ('127.0.0.1', 2130706433),
                       ('137.12.56.34', -1995687902),
                       ('202.65.43.21', -901698795),
                       ('255.255.255.255', -1)]
        for sip, iip in ips_answers:
            self.assertEqual(iputils.ip_to_signed_int(sip), iip)

    def test_signed_int_to_ip(self):
        ips_answers = [('0.0.0.1', 1),
                       ('59.123.45.67', 997928259),
                       ('127.0.0.1', 2130706433),
                       ('137.12.56.34', -1995687902),
                       ('202.65.43.21', -901698795),
                       ('255.255.255.255', -1)]
        for sip, iip in ips_answers:
            self.assertEqual(iputils.signed_int_to_ip(iip), sip)

    def test_ip_to_unsigned_int(self):
        ips_answers = [('0.0.0.1', 1),
                       ('59.123.45.67', 997928259),
                       ('127.0.0.1', 2130706433),
                       ('137.12.56.34', 2299279394),
                       ('202.65.43.21', 3393268501),
                       ('255.255.255.255', long(0xFFFFFFFF))]
        for sip, iip in ips_answers:
            self.assertEqual(iputils.ip_to_unsigned_int(sip), iip)

    def test_unsigned_int_to_signed_int(self):
        ips_answers = [(0, 0),
                       (1, 1),
                       (997928259, 997928259),
                       (2130706433, 2130706433),
                       (2147483647, 2147483647),
                       (2147483648, -2147483648),
                       (2299279394, -1995687902),
                       (3393268501, -901698795),
                       (long(0xFFFFFFFF), -1)]
        for uip, sip in ips_answers:
            self.assertEqual(iputils.unsigned_int_to_signed_int(uip), sip)

    def test_signed_int_to_unsigned_int(self):
        ips_answers = [(0, 0),
                       (1, 1),
                       (997928259, 997928259),
                       (2130706433, 2130706433),
                       (2147483647, 2147483647),
                       (2147483648, -2147483648),
                       (2299279394, -1995687902),
                       (3393268501, -901698795),
                       (long(0xFFFFFFFF), -1)]
        for uip, sip in ips_answers:
            self.assertEqual(iputils.signed_int_to_unsigned_int(sip), uip)

    def test_ipv6_to_unsigned_int(self):
        ips_answers = [('::ffff:ffff:ffff:ffff', 18446744073709551615),
                       ('::FFFF:FFFF', 4294967295),
                       ('::255.255.255.255', 4294967295),
                       ('2002::', 42545680458834377588178886921629466624),
                       ('::ffff:192.0.2.128', 281473902969472)]
        for sip, iip in ips_answers:
            self.assertEqual(iputils.ipv6_to_unsigned_int(sip), iip)

    def test_any_ip_to_unsigned_int(self):
        ips_answers = [('0.0.0.1', 1),
                       ('59.123.45.67', 997928259),
                       ('127.0.0.1', 2130706433),
                       ('137.12.56.34', 2299279394),
                       ('202.65.43.21', 3393268501),
                       ('255.255.255.255', long(0xFFFFFFFF)),
                       ('::ffff:ffff:ffff:ffff', 18446744073709551615),
                       ('::ffff:ffff', 4294967295),
                       ('::255.255.255.255', 4294967295),
                       ('2002::', 42545680458834377588178886921629466624),
                       ('::ffff:192.0.2.128', 281473902969472)]
        for sip, iip in ips_answers:
            self.assertEqual(iputils.any_ip_to_unsigned_int(sip), iip)

    def test_unsigned_int_to_ip(self):
        ips_answers = [('0.0.0.1', 1),
                       ('59.123.45.67', 997928259),
                       ('127.0.0.1', 2130706433),
                       ('137.12.56.34', 2299279394),
                       ('202.65.43.21', 3393268501),
                       ('255.255.255.255', long(0xFFFFFFFF))]
        for sip, iip in ips_answers:
            self.assertEqual(iputils.unsigned_int_to_ip(iip), sip)

    def test_unsigned_int_to_ipv6(self):
        ips_answers = [('::ffff:ffff:ffff:ffff', 18446744073709551615),
                       ('::255.255.255.255', 4294967295),
                       ('2002::', 42545680458834377588178886921629466624),
                       ('::ffff:192.0.2.128', 281473902969472)]
        for sip, iip in ips_answers:
            self.assertEqual(iputils.unsigned_int_to_ipv6(iip), sip)

    def test_is_ip_in_range(self):
        ip_ip_sets = [
            ('123.45.67.89', set(['127.0.0.1', '123.45', '255']), True),
            ('123.45.67.89', set(['127.0.0.1', '121.45', '255']), False)]
        for sip, ip_set, expected_result in ip_ip_sets:
            self.assertEqual(
                iputils.is_ip_in_range(sip, ip_set), expected_result)

    def test_get_address_family(self):
        ips_answers = [('0.0.0.1', socket.AF_INET),
                       ('59.123.45.67', socket.AF_INET),
                       ('127.0.0.1', socket.AF_INET),
                       ('137.12.56.34', socket.AF_INET),
                       ('202.65.43.21', socket.AF_INET),
                       ('255.255.255.255', socket.AF_INET),
                       ('::ffff:ffff:ffff:ffff', socket.AF_INET6),
                       ('::ffff:ffff', socket.AF_INET6),
                       ('::255.255.255.255', socket.AF_INET6),
                       ('2002::', socket.AF_INET6),
                       ('::ffff:192.0.2.128', socket.AF_INET6)]
        for sip, af in ips_answers:
            self.assertEqual(iputils.get_address_family(sip), af)

    def test_is_valid_ip(self):
        ips_answers = [('0.0.0.0', True), ('123.45.67.89', True),
                       ('255.255.255.255', True), ('268.1.43.89', False),
                       ('ab.d', False), ('2.456.9.0', False),
                       ('123123123123', False), ('123.123.123.1234', False),
                       ('278', False), ('1.2', False), ('01.02.03.04', False),
                       ('1.2.3.4.5', False), ('-1', False)]
        for sip, expected_result in ips_answers:
            self.assertEqual(iputils.is_valid_ip(sip), expected_result)

    def test_is_valid_ipv6(self):
        ips_answers = [('::ffff:192.0.2.0x80', False),
                       ('::ffff:192.0.2.128', True),
                       ('0xffffc0000280', False),
                       ('fe80:0000:0000:0000:0202:b3ff:fe1e:8329', True),
                       ('fe80:0:0:0:202:b3ff:fe1e:8329', True),
                       ('fe80::202:b3ff:fe1e:8329', True),
                       ('::ffff:192.0.2.128', True)]
        for sip, expected_result in ips_answers:
            self.assertEqual(iputils.is_valid_ipv6(sip), expected_result)

    def test_is_any_ip(self):
        ips_answers = [('0.0.0.0', True), ('123.45.67.89', True),
                       ('255.255.255.255', True), ('268.1.43.89', False),
                       ('ab.d', False), ('2.456.9.0', False),
                       ('123123123123', False), ('123.123.123.1234', False),
                       ('278', False), ('1.2', False), ('01.02.03.04', False),
                       ('1.2.3.4.5', False), ('-1', False),
                       ('::ffff:192.0.2.0x80', False),
                       ('::ffff:192.0.2.128', True),
                       ('0xffffc0000280', False),
                       ('fe80:0000:0000:0000:0202:b3ff:fe1e:8329', True),
                       ('fe80:0:0:0:202:b3ff:fe1e:8329', True),
                       ('fe80::202:b3ff:fe1e:8329', True),
                       ('::ffff:192.0.2.128', True)]
        for sip, expected_result in ips_answers:
            self.assertEqual(iputils.is_any_ip(sip), expected_result)

    def test_is_valid_cidr(self):
        ips_answers = [('0.0.0.0/8', True), ('123.45.67.89/31', True),
                       ('255.255.255.255/33', False), ('268.1.43.89/16', False),
                       ('-1', False), ('2.456.9.0/8', False)]
        for sip, expected_result in ips_answers:
            self.assertEqual(iputils.is_valid_cidr(sip), expected_result)

    def test_is_routable_ip(self):
        routable = [ '69.17.49.217', '209.237.228.10' ]
        unroutable = [
            '127.0.0.1',
            '10.1.1.1',
            '172.16.45.12',
            '192.168.1.1',
            '169.254.12.13',
            '228.0.0.1',
            u'69.17.49.217\u0128',
            '198.51.100.0',
            '203.0.113.0',
            '100.64.0.0',
            '100.127.0.0'
            ]
        for sip in routable:
            self.assertTrue(iputils.is_routable_ip(sip),
                            'routable IP (%s) reported unroutable' % sip)
        for sip in unroutable:
            self.assertFalse(iputils.is_routable_ip(sip),
                             'unroutable IP (%s) reported routable' % sip)

    def test_is_routable_ipv6(self):
        routable = ['20ab:0db8:85a3:0000:0000:8a2e:0370:7334',
                    '::2',
                    '::1:0:0:0',
                    '::fffe:0:0',
                    '::1:ffff:0:0',
                    '::fff:0:0',
                    'fe7f::',
                    'fec0::',
                    'fe8::',
                    '::fe80',
                    'fbff::',
                    'fe00::',
                    'fc::',
                    'fc0::',
                    '2001:db7::',
                    '2001:db9::',
                    '2001:db80::',
                    '0:2001:db8::',
                    '2001:9::',
                    '2001:20::',
                    '2001:100::',
                    '0:2001:10::',
                    '2001:2:1::',
                    '2001:20::',
                    '0:2001:2::']
        unroutable = ['::1',
                      '::',
                      '::ffff:0:0',
                      '::ffff:ffff:ffff',
                      '::1.2.3.4',
                      'fe80::',
                      'febf::',
                      'fc00::',
                      'fdff::',
                      '2001:db8::',
                      '2001:db8:ffff::',
                      '2001:10::',
                      '2001:1f::',
                      '2001:2::',
                      '2001:2:0:abcd::',
                      'abcd:g::']
        for sip in routable:
            self.assertTrue(iputils.is_routable_ipv6(sip),
                            'routable IPv6 (%s) reported unroutable' % sip)
        for sip in unroutable:
            self.assertFalse(iputils.is_routable_ipv6(sip),
                             'unroutable IPv6 (%s) reported routable' % sip)

    def test_is_routable_any_ip(self):
        routable = ['69.17.49.217',
                    '209.237.228.10',

                    '20ab:0db8:85a3:0000:0000:8a2e:0370:7334',
                    '::2',
                    '::1:0:0:0',
                    '::fffe:0:0',
                    '::1:ffff:0:0',
                    '::fff:0:0',
                    'fe7f::',
                    'fec0::',
                    'fe8::',
                    '::fe80',
                    'fbff::',
                    'fe00::',
                    'fc::',
                    'fc0::',
                    '2001:db7::',
                    '2001:db9::',
                    '2001:db80::',
                    '0:2001:db8::',
                    '2001:9::',
                    '2001:20::',
                    '2001:100::',
                    '0:2001:10::',
                    '2001:2:1::',
                    '2001:20::',
                    '0:2001:2::']
        unroutable = ['127.0.0.1',
                      '10.1.1.1',
                      '172.16.45.12',
                      '192.168.1.1',
                      '169.254.12.13',
                      '228.0.0.1',
                      u'69.17.49.217\u0128',

                      '::1',
                      '::',
                      '::ffff:0:0',
                      '::ffff:ffff:ffff',
                      '::1.2.3.4',
                      'fe80::',
                      'febf::',
                      'fc00::',
                      'fdff::',
                      '2001:db8::',
                      '2001:db8:ffff::',
                      '2001:10::',
                      '2001:1f::',
                      '2001:2::',
                      '2001:2:0:abcd::',
                      'abcd:g::']
        for sip in routable:
            self.assertTrue(iputils.is_routable_any_ip(sip),
                            'routable IP (%s) reported unroutable' % sip)
        for sip in unroutable:
            self.assertFalse(iputils.is_routable_any_ip(sip),
                             'unroutable IP (%s) reported routable' % sip)

    def test_is_illegal_source_ip(self):
        illegal = [ '0.0.0.0', '2.1.2.3', '169.254.1.1' ]
        legal = [ '69.17.49.217', '209.237.228.10' ]
        for sip in illegal:
            self.assertTrue(iputils.is_illegal_source_ip(sip))
        for sip in legal:
            self.assertFalse(iputils.is_illegal_source_ip(sip))

    def test_cidr_to_range(self):
        ips = [('153.106.4.1', '153.106.4.1', '153.106.4.1'),
               ('153.106', '153.106.0.0', '153.106.255.255'),
               ('153.106.*.*', '153.106.0.0', '153.106.255.255'),
               ([153, 106], '153.106.0.0', '153.106.255.255'),
               ((127, '0', '0', 1), '127.0.0.1', '127.0.0.1'),
               ('153.106.0.0/16', '153.106.0.0', '153.106.255.255')]
        for sip, lower, upper in ips:
            self.assertEqual(iputils.cidr_to_range(sip),
                             (iputils.ip_to_unsigned_int(lower),
                              iputils.ip_to_unsigned_int(upper)))
            self.assertEqual(iputils.cidr_to_range(sip, signed=True),
                             (iputils.ip_to_signed_int(lower),
                              iputils.ip_to_signed_int(upper)))
        self.assertRaises(TypeError, iputils.cidr_to_range, 200000)

    def test_ipv6_cidr_to_range(self):
        ips = [('20ab:0db8:85a3:0000:0000:8a2e:0370:7334', '20ab:0db8:85a3:0000:0000:8a2e:0370:7334', '20ab:0db8:85a3:0000:0000:8a2e:0370:7334'),
               ('::1', '::1', '::1'),
               ('20ab:0db8:85a3:0000:0000:8a2e:0370:7334/16', '20ab::', '20ab:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
               ('20ab:0db8:85a3:0000:0000:8a2e:0370:7334/0', '::', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
               ('::1/128', '::1', '::1')]
        for sip, lower, upper in ips:
            self.assertEqual(iputils.ipv6_cidr_to_range(sip),
                             (iputils.ipv6_to_unsigned_int(lower),
                              iputils.ipv6_to_unsigned_int(upper)))
        self.assertRaises(ValueError, iputils.ipv6_cidr_to_range, '20ab:0db8:85a3:0000:0000:8a2e:0370:7334/150')
        self.assertRaises(ValueError, iputils.ipv6_cidr_to_range, '20ab:0db8:85a3:0000:0000:8a2e:0370')

    def test_any_cidr_to_range(self):
        ips = [('153.106.4.1', '153.106.4.1', '153.106.4.1'),
               ('153.106', '153.106.0.0', '153.106.255.255'),
               ('153.106.*.*', '153.106.0.0', '153.106.255.255'),
               ([153, 106], '153.106.0.0', '153.106.255.255'),
               ((127, '0', '0', 1), '127.0.0.1', '127.0.0.1'),
               ('153.106.0.0/16', '153.106.0.0', '153.106.255.255'),
               ('20ab:0db8:85a3:0000:0000:8a2e:0370:7334', '20ab:0db8:85a3:0000:0000:8a2e:0370:7334', '20ab:0db8:85a3:0000:0000:8a2e:0370:7334'),
               ('::1', '::1', '::1'),
               ('20ab:0db8:85a3:0000:0000:8a2e:0370:7334/16', '20ab::', '20ab:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
               ('20ab:0db8:85a3:0000:0000:8a2e:0370:7334/0', '::', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'),
               ('::1/128', '::1', '::1')]
        for sip, lower, upper in ips:
            self.assertEqual(iputils.any_cidr_to_range(sip),
                             (iputils.any_ip_to_unsigned_int(lower),
                              iputils.any_ip_to_unsigned_int(upper)))

    def test_cidr_to_ip_range_hex(self):
        expected_results = [
            ('192.168.1.1', ('c0a80101', 'c0a80101')),
            ('213.172.0x1f.13', ('d5ac1f0d', 'd5ac1f0d')),
            ('12.2.4.0/24', ('0c020400', '0c0204ff')),
            ('0xaf.13.0.0/16', ('af0d0000', 'af0dffff'))]

        for scidr, expected_result in expected_results:
            self.assertEqual(iputils.cidr_to_ip_range_hex(scidr), expected_result)

    def test_make_mask(self):
        bits_mask = [(32, 0xFFFFFFFF),
                     (28, 0xFFFFFFF0),
                     (24, 0xFFFFFF00),
                     (20, 0xFFFFF000),
                     (16, 0xFFFF0000),
                     (12, 0xFFF00000),
                     (8, 0xFF000000),
                     (4, 0xF0000000),
                     (0, 0x00000000)]
        for bits, mask in bits_mask:
            self.assertEqual(iputils.make_mask(bits), mask)

        self.assertRaises(ValueError, iputils.make_mask, 34)
        self.assertRaises(ValueError, iputils.make_mask, -2)

    def test_make_mask_ipv6(self):
        bits_mask = [(128, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF),
                     (124, 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0),
                     (16, 0xFFFF0000000000000000000000000000),
                     (0, 0x00000000000000000000000000000000)]
        for bits, mask in bits_mask:
            self.assertEqual(iputils.make_mask_ipv6(bits), mask)

        self.assertRaises(ValueError, iputils.make_mask_ipv6, 129)
        self.assertRaises(ValueError, iputils.make_mask_ipv6, -1)

    def test_ip_range_to_cidr(self):
        ips_range = [
            ('153.106.4.0', '153.106.4.255', set([('153.106.4.0', 24), ])),
            ('153.106.4.0', '153.106.4.1', set([('153.106.4.0', 31), ])),
            ('153.106.4.0', '153.106.4.4', set([('153.106.4.0', 30),
                                               ('153.106.4.4', 32), ])),
            ]
        for start_ip, end_ip, range_ip in ips_range:
            s = iputils.ip_to_unsigned_int(start_ip)
            e = iputils.ip_to_unsigned_int(end_ip)
            cidrs = iputils.ip_range_to_cidr(s, e)
            cidr_set = set()
            for sip, cidr in cidrs:
                cidr_set.add((iputils.unsigned_int_to_ip(sip), cidr))
            self.assertEqual(range_ip - cidr_set, set())

    def test_subtract_cidr_list(self):
        cidrs_answers = [
            ('153.106.4.0/31', ['153.106.4.0/32'], ['153.106.4.1/32']),
            ('153.106.0.0/23', ['153.106.0.24/24'], ['153.106.1.0/24']), ]
        for source, cidr_list, expected_result in cidrs_answers:
            result = iputils.subtract_cidr_list(source, cidr_list)
            l_result = [ r for r in result ]
            self.assertEqual(l_result, expected_result)

    def test_star_to_cidr(self):
        star_cidr = [('1.2.3.*', '1.2.3.0/24'),
                     ('127.*', '127.0.0.0/8'),
                     ('153.106.4.1', '153.106.4.1/36')]
        for star, cidr in star_cidr:
            self.assertEqual(iputils.star_to_cidr(star), cidr)

    def test_blank_to_cidr(self):
        blank_cidr = [('1.2.3', '1.2.3.0/24'),
                      ('127', '127.0.0.0/8'),
                      ('153.106.4.1', '153.106.4.1')]
        for blank, cidr in blank_cidr:
            self.assertEqual(iputils.blank_to_cidr(blank), cidr)

    def test_reverse_ip(self):
        ip_reverse = [('1.2.3.4', '4.3.2.1'), ('127.0.0.1', '1.0.0.127')]
        for sip, r_ip in ip_reverse:
            self.assertEqual(iputils.reverse_ip(sip), r_ip)

    def test_canonify_ipv6_str(self):
        expected_results = [
            ('20ab:0db8:85a3:0000:0000:8a2e:0370:7334', '20ab:db8:85a3::8a2e:370:7334'),
            ('0000:0000:0000::1234', '::1234'),
            ('::FFFF', '::ffff'),
            ('20ab:000a::', '20ab:a::'),
            ('20ab:db8:85a3:1000:2000:8a2e:1370:7334', '20ab:db8:85a3:1000:2000:8a2e:1370:7334')
        ]

        for sdom_str, expected_result in expected_results:
            self.assertEqual(iputils.canonify_ipv6_str(sdom_str), expected_result)

        self.assertRaises(ValueError, iputils.canonify_ipv6_str, '20ab')

    def test_canonify_ip_str(self):
        expected_results = [
            ('213.172.0x1f.13', '213.172.31.13'),
            ('::ffff:192.0.2.0x80', '::ffff:192.0.2.128'),
            ('0x7f000001', '127.0.0.1'),
            ('0xffffc0000280', '::ffff:192.0.2.128'),
            ('3189439978', '190.26.253.234'),
            ('281473902969472', '::ffff:192.0.2.128')]

        for sdom_str, expected_result in expected_results:
            self.assertEqual(iputils.canonify_ip_str(sdom_str), expected_result)

        self.assertRaises(iputils.InvalidIpStrError, iputils.canonify_ip_str, '23.12.0x1z.13')
        self.assertRaises(iputils.InvalidIpStrError, iputils.canonify_ip_str, '0x1x.13.123.125')

    def test_apply_cidr_iip(self):
        # 123.123.123.123/16 = 123.123.0.0
        input_iip = 2071690107
        output_iip = 2071658496
        self.assertEqual(iputils.apply_cidr_iip(input_iip, 16), output_iip)

        # 123.123.123.123/8 = 123.0.0.0
        input_iip = 2071690107
        output_iip = 2063597568
        self.assertEqual(iputils.apply_cidr_iip(input_iip, 8), output_iip)

    def test_apply_cidr_iip6(self):
        # abcd:1234:5678:90ef::1234/32 = abcd:1234::
        input_iip = 228362777365219036774747743227027132980L
        output_iip = 228362777338457570402212944970237607936L
        self.assertEqual(iputils.apply_cidr_iip6(input_iip, 32), output_iip)

        # abcd:1234:5678:90ef::1234/64 = abcd:1234:5678:90ef::
        input_iip = 228362777365219036774747743227027132980L
        output_iip = 228362777365219036774747743227027128320L
        self.assertEqual(iputils.apply_cidr_iip6(input_iip, 64), output_iip)

    def test_apply_cidr_any_sip(self):
        input_sip = '123.123.123.123'
        output_sip = '123.123.0.0'
        self.assertEqual(iputils.apply_cidr_any_sip(input_sip, 16), output_sip)

        input_sip = '123.123.123.123'
        output_sip = '123.0.0.0'
        self.assertEqual(iputils.apply_cidr_any_sip(input_sip, 8), output_sip)

        input_sip = 'abcd:1234:5678:90ef::1234'
        output_sip = 'abcd:1234::'
        self.assertEqual(iputils.apply_cidr_any_sip(input_sip, 32), output_sip)

        input_sip = 'abcd:1234:5678:90ef::1234'
        output_sip = 'abcd:1234:5678:90ef::'
        self.assertEqual(iputils.apply_cidr_any_sip(input_sip, 64), output_sip)

    def test_investigate_ip(self):
        for row in self.IP_TEST_DATA:
            expecting = dict(zip(self.IP_TEST_COLUMNS, row))
            verdict = iputils.investigate_ip(expecting['test_ip'])

            del expecting['test_ip']
            del expecting['comment']
            self.assertEqual(verdict, expecting)

    def test_reverse_dns(self):
        expected_results = [
            ('198.133.219.25', '25.219.133.198.in-addr.arpa'),
            ('69.17.49.217', '217.49.17.69.in-addr.arpa'),
            ('209.237.228.10', '10.228.237.209.in-addr.arpa'),
            ('2001:67:ac:150::250',
             '0.5.2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.5.1.0.c.a.0.0.7.6.0.0.1.0.0.2.ip6.arpa'),
            ('2001:4ba0:fff7:5d::2',
             '2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.d.5.0.0.7.f.f.f.0.a.b.4.1.0.0.2.ip6.arpa'),
            ('2400:b800:1:1:20c:29ff:fe2c:3d91',
             '1.9.d.3.c.2.e.f.f.f.9.2.c.0.2.0.1.0.0.0.1.0.0.0.0.0.8.b.0.0.4.2.ip6.arpa')]

        for ip_str, expected_result in expected_results:
            self.assertEqual(iputils.reverse_dns(ip_str), expected_result)

    def test_reverse_dns_pieces_ipv4(self):
        expected_results = [
            ('198.133.219.25', None, '25.219.133.198'),
            ('69.17.49.217', 'suffix', '217.49.17.69.suffix'),
            ('209.237.228.10', '', '10.228.237.209')]

        for ip_str, suffix, expected_result in expected_results:
            if suffix is not None:
                self.assertEqual(
                        iputils.reverse_dns_pieces_ipv4(ip_str, suffix),
                        expected_result)
            else:
                self.assertEqual(iputils.reverse_dns_pieces_ipv4(ip_str),
                                 expected_result)

    def test_reverse_dns_pieces_ipv6(self):
        expected_results = [
            ('2001:67:ac:150::250', None,
             '0.5.2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.5.1.0.c.a.0.0.7.6.0.0.1.0.0.2'),
            ('2001:4ba0:fff7:5d::2', 'suffix',
             '2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.d.5.0.0.7.f.f.f.0.a.b.4.1.0.0.2.suffix'),
            ('2400:b800:1:1:20c:29ff:fe2c:3d91', '',
             '1.9.d.3.c.2.e.f.f.f.9.2.c.0.2.0.1.0.0.0.1.0.0.0.0.0.8.b.0.0.4.2')]

        for ip_str, suffix, expected_result in expected_results:
            if suffix is not None:
                self.assertEqual(
                        iputils.reverse_dns_pieces_ipv6(ip_str, suffix),
                        expected_result)
            else:
                self.assertEqual(iputils.reverse_dns_pieces_ipv6(ip_str),
                                 expected_result)

    def test_reverse_dns_pieces(self):
        expected_results = [
            ('198.133.219.25', None, '25.219.133.198'),
            ('69.17.49.217', 'suffix', '217.49.17.69.suffix'),
            ('209.237.228.10', '', '10.228.237.209'),
            ('2001:67:ac:150::250', None,
             '0.5.2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.5.1.0.c.a.0.0.7.6.0.0.1.0.0.2'),
            ('2001:4ba0:fff7:5d::2', 'suffix',
             '2.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.d.5.0.0.7.f.f.f.0.a.b.4.1.0.0.2.suffix'),
            ('2400:b800:1:1:20c:29ff:fe2c:3d91', '',
             '1.9.d.3.c.2.e.f.f.f.9.2.c.0.2.0.1.0.0.0.1.0.0.0.0.0.8.b.0.0.4.2')]

        for ip_str, suffix, expected_result in expected_results:
            if suffix is not None:
                self.assertEqual(iputils.reverse_dns_pieces(ip_str, suffix),
                                 expected_result)
            else:
                self.assertEqual(iputils.reverse_dns_pieces(ip_str),
                                 expected_result)


if __name__ == "__main__":
    unittest.main()
