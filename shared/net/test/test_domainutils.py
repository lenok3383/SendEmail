#!/usr/bin/env python
"""Test the domain module.
:Status: $Id: //prod/main/_is/shared/python/net/test/test_domainutils.py#5 $
:Author: vscherb
"""

# Relies on unittest2 package
import unittest2 as unittest

from shared.net import domainutils
from shared.net import iputils
from shared.net import registrar

class DomainTestCase(unittest.TestCase):

    def setUp(self):
        # Each app which is using registrar should init it by its own means.
        registrar.init_registrar()


    def test_reverse_domain(self):
        expected_results = [
            ('www.google.com', 'com.google.www'),
            ('foo.bar', 'bar.foo')]

        for sdomain, expected_result in expected_results:
            self.assertEqual(domainutils.reverse_domain(sdomain), expected_result)


    def test_ascii_encode(self):
        expected_results = [
            ('',''),
            ('asdf','asdf'),
            ('\xab','%ab'),
            ('%ab','%ab'),
            (u'\xab','%c2%ab'),
            (u'\u00ab','%c2%ab'),
            ('word1 word2%20word3','word1 word2%20word3'),
            ('\x50\x00\x51','P\0Q'),
            ('a\xab\xcd%efbc\x80','a%ab%cd%efbc%80'),
            (u'\u0123','%c4%a3'),
            (u'\u1234','%e1%88%b4'),
            (u'\u1234\u5678','%e1%88%b4%e5%99%b8'),
            (u'asdf','asdf'),
            (u'asdf\uabcdjkl\x80','asdf%ea%af%8djkl%c2%80')]

        for splain_str, expected_result in expected_results:
            self.assertEqual(domainutils.ascii_encode(splain_str), expected_result)

        self.assertRaises(ValueError, domainutils.ascii_encode, 120000)


    def test_interpret_domain_as_ip(self):
        expected_results = [
            ('1.2.3.4', '1.2.3.4'),
            # Remove brackets in case of IPv6 domain string
            ('[2001:db8:11a3:9d7:1f34:8a2e:7a0:765d]', '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'),
            # Odd look urls in IE or Outlook will be convert to decimal IP addresses
            ('213.172.0x1f.13', '213.172.31.13'),
            ('0x7f000001', '127.0.0.1'),
            ('1113343453', '66.92.69.221'),
            ('[::ffff:192.0.2.0x80]', '::ffff:192.0.2.128'),
            ('[281473902969472]', '::ffff:192.0.2.128'),
            ('[0xffffc0000280]', '::ffff:192.0.2.128')]

        for dom_str, expected_result in expected_results:
            self.assertEqual(domainutils._interpret_domain_as_ip(dom_str), expected_result)

        # Raise InvalidIpStrError in case of invalid IP domain string
        self.assertRaises(iputils.InvalidIpStrError, domainutils._interpret_domain_as_ip, 'boo.foo')
        self.assertRaises(iputils.InvalidIpStrError, domainutils._interpret_domain_as_ip, '1.2.3.4.256')
        # Raise InvalidDomainError in case of invalid IPv6 domain string
        self.assertRaises(domainutils.InvalidDomainError, domainutils._interpret_domain_as_ip, '[::ffff:192.0.2.0xYZ]')
        self.assertRaises(domainutils.InvalidDomainError, domainutils._interpret_domain_as_ip, 'boo[:]foo')

    def test_split_reversed_dom_str(self):
        expected_results = [
            # An IP
            ('1.2.3.4', ('1.2.3.4', '')),

            # No subdomain
            ('com.foo', ('com.foo', '')),

            # One sub-domain
            ('com.foo.bar', ('com.foo', 'bar')),

            # Two sub-domain
            ('com.foo.bar.haha', ('com.foo', 'bar.haha')),

            # Foreign country top level domain
            ('cn.tianya.www', ('cn.tianya', 'www')),
            ('cn.net.cnnic',('cn.net.cnnic', '')),

            # Foreign country + common domain + sub
            ('cn.com.tianya.www', ('cn.com.tianya', 'www')),
            ('cn.net.cnnic.host1', ('cn.net.cnnic', 'host1')),
            ('cn.net.cnnic.www', ('cn.net.cnnic', 'www')),

            # Foreign country domain without sub domain
            ('cn.net.cnnic', ('cn.net.cnnic', '')),

            # Foreign country domain + two sub domains
            ('cn.net.cnnic.something.www', ('cn.net.cnnic', 'something.www'))]

        for sdom_str, expected_result in expected_results:
            self.assertEqual(domainutils.split_reversed_dom_str(sdom_str), expected_result)

        # Domain should have at least two syllables
        self.assertRaises(domainutils.InvalidDomainError, domainutils.split_reversed_dom_str, 'foo')
        # Domain name must have at least one '.' (two syllables)
        self.assertRaises(domainutils.InvalidDomainError, domainutils.split_reversed_dom_str, 'com.')
        # Raise InvalidDomainError in case of invalid IPv6 domain string
        self.assertRaises(domainutils.InvalidDomainError, domainutils.split_reversed_dom_str, 'http://[::ffff:192.0.2.0xYZ]/')


    def test_split_normal_dom_str(self):
        expected_results = [
            (('1.2.3.4', True), ('1.2.3.4', '')),
            (('192.168.1.1',), ('192.168.1.1', '')),
            (('www.maps.google.com', True), ('com.google', 'maps.www')),
            (('bla.foo.bar.com', False), ('bar.com', 'bla.foo'))]

        for sdom_str, expected_result in expected_results:
            self.assertEqual(domainutils.split_normal_dom_str(*sdom_str), expected_result)

        # Domain should have at least two syllables
        self.assertRaises(domainutils.InvalidDomainError, domainutils.split_normal_dom_str, 'spam')
        # Raise InvalidDomainError in case of invalid IPv6 domain string
        self.assertRaises(domainutils.InvalidDomainError, domainutils.split_normal_dom_str, 'http://[::ffff:192.0.2.0xYZ]/')


    def test_split_url(self):
        expected_results = [
            # Remove both leading and tailing empty spaces
            ({'url':'  http://foo.bar '}, ('bar.foo', '', '', 'http', '')),

            # Add schema for host name starts with ftp.
            ({'url':'ftp.foo.bar'}, ('bar.foo', 'ftp', '', 'ftp', '')),

            # Add http schema for host name not starts with ftp.
            ({'url':'www.foo.bar'}, ('bar.foo', 'www', '', 'http', '')),

            # Correctly unquoting urls using urllib.unquote_plus when protocol contains
            # quoted characters
            ({'url':'http%3A%2F%2F971.cleverreach.de%2Fc%2F542553%2FcZmjo52VpY8%3D'},
                   ('de.cleverreach', '971', '/c/542553/czmjo52vpy8=', 'http', '')),

            # If the host part is IP, all goes to domain
            ({'url':'http://1.2.3.4'}, ('1.2.3.4', '', '', 'http', '')),

            # Odd look urls in IE or Outlook will be convert to decimal IP addresses
            ({'url':'http://213.172.0x1f.13/'}, ('213.172.31.13', '', '', 'http', '')),
            ({'url':'http://0x7f000001/'}, ('127.0.0.1', '', '', 'http', '')),
            ({'url':'http://1113343453/'}, ('66.92.69.221', '', '', 'http', '')),

            # Fix urls forget //
            # http:www.foo.biz -> http://www.foo.biz
            ({'url':'http:www.foo.biz'}, ('biz.foo', 'www', '', 'http', '')),

            # Unquote %xx if unquote_url == True (default)
            # unqoute + to ' '
            ({'url':'http://foo.bar/%7Eted+cui'}, ('bar.foo', '', '/~ted cui', 'http', '')),
            ({'url':'http://foo.bar/%7Eted+cui', 'unquote_url':False, 'lower_path':False},
                   ('bar.foo', '', '/%7Eted+cui', 'http', '')),

            # Add slash in front of ? if path is missing
            # http://www.foo.biz?id=3 -> http://www.foo.biz/?id=3
            ({'url':'http://www.foo.biz?id=3', 'remove_tailing_slash':False},
                   ('biz.foo', 'www', '/', 'http', '')),

            # Ddomain is always be lower cased
            ({'url':'http://WWW.Tcui.Org'}, ('org.tcui', 'www', '', 'http', '')),

            # Lower path if lower_path == True (default)
            ({'url':'http://foo.com/Bar'}, ('com.foo', '', '/bar', 'http', '')),
            ({'url':'http://foo.com/Bar', 'lower_path':False},
                   ('com.foo', '', '/Bar', 'http', '')),

            # Remove tailing '/' when remove_tailing_slash == True (default)
            ({'url':'http://foo.bar/'}, ('bar.foo', '', '', 'http', '')),
            ({'url':'http://foo.bar/', 'remove_tailing_slash':False},
                   ('bar.foo', '', '/', 'http', '')),
            ({'url':'http://foo.bar/abc/', 'remove_tailing_slash':False},
                   ('bar.foo', '', '/abc/', 'http', '')),

            # Convert path to hash if  hash_path == True (default is False)
            ({'url':'http://foo.com/abc'}, ('com.foo', '', '/abc', 'http', '')),
            ({'url':'http://foo.com/abc', 'hash_path':True},
                   ('com.foo', '', '/900150983cd2', 'http', '')),
            ({'url':'http://foo.bar/abc//bb///ccc', 'hash_path':True},
                   ('bar.foo', '', '/900150983cd2/21ad0bd836b9/9df62e693988', 'http', '')),

            # Return extra data when return_extra_parts is true
            ({'url':'ftp://cartao:virtual@82.88.114.91', 'return_extra_parts':True},
                   ('82.88.114.91', '', '', 'ftp', '', '', '', 'cartao:virtual')),
            ({'url':'ftp://musasdanet2006:250272@ftp.musasdanet2006.netfirms.com', 'return_extra_parts':True},
                   ('com.netfirms', 'musasdanet2006.ftp', '', 'ftp', '', '', '', 'musasdanet2006:250272')),
            ({'url':'http://me:passwd@www.foo.com:599/bar?query=x&b=c#fregment', 'return_extra_parts':True},
                   ('com.foo', 'www', '/bar', 'http', '599', 'query=x&b=c', 'fregment', 'me:passwd')),
            ({'url':'me:passwd@www.f001.foo.com:599/bar?query=x&b=c#fregment', 'return_extra_parts':True},
                   ('com.foo', 'f001.www', '/bar', 'http', '599', 'query=x&b=c', 'fregment', 'me:passwd'))]

        for ssplit_url, expected_result in expected_results:
            self.assertEqual(domainutils.split_url(**ssplit_url), expected_result)

        # Domain must have at least two syllables
        self.assertRaises(domainutils.InvalidDomainError, domainutils.split_url, 'com.')
        self.assertRaises(domainutils.InvalidDomainError, domainutils.split_url, 'localhost')

        # Port must be int
        self.assertRaises(ValueError, domainutils.split_url, 'http://foo.bar:abc')

        # No double (or more) ports
        self.assertRaises(domainutils.InvalidDomainError, domainutils.split_url, 'http://foo.com:80:8080')

        # Protocol must be valid
        self.assertRaises(domainutils.InvalidProtocolError, domainutils.split_url, 'httpp://boo.foo')


    def test_split_url_with_ipv6(self):
        expected_results = [
            ('http://[2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d]/', ('2001:db8:11a3:9d7:1f34:8a2e:7a0:765d', '', '', 'http', '')),
            ('http://[2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d]:8080/', ('2001:db8:11a3:9d7:1f34:8a2e:7a0:765d', '', '', 'http', '8080')),

            # Zeroes can be omitted
            ('http://[::ae21:ad12]/', ('::174.33.173.18', '', '', 'http', '')),

            # The IPv4-mapped IPv6 addresses having the last 32 bits written
            # in the customary dot-decimal notation of IPv4
            ('http://[::ffff:192.0.2.128]/', ('::ffff:192.0.2.128', '', '', 'http', '')),

            # Odd look urls in IE or Outlook will be convert to decimal IP addresses
            ('http://[::ffff:192.0.2.0x80]/', ('::ffff:192.0.2.128', '', '', 'http', '')),
            ('http://[281473902969472]/', ('::ffff:192.0.2.128', '', '', 'http', '')),
            ('http://[0xffffc0000280]/', ('::ffff:192.0.2.128', '', '', 'http', ''))]

        self.assertRaises(domainutils.InvalidDomainError, domainutils.split_url, 'http://[::ffff:192.0.2.0xYZ]/')
        self.assertRaises(domainutils.InvalidDomainError, domainutils.split_url, 'http://2001:67:ac:150::250/')


    def test_unsplit_url(self):
        expected_results = [
            (('foo.com', 'www', '/bar', 'http', '599', 'query=x&b=c', 'fregment', 'me:passwd'),
                  'http://me:passwd@www.foo.com:599/bar?query=x&b=c#fregment'),
            (('82.88.114.91', '', '', 'ftp', '', '', '', 'cartao:virtual'),
                  'ftp://cartao:virtual@82.88.114.91'),
            (('bar.foo', '', '/abc//bb///ccc', 'http', '', '', '', ''),
                  'http://bar.foo/abc//bb///ccc'),
            (('82.88.114.91', '', '', 'ftp', '', '', '', 'cartao:virtual'),
                  'ftp://cartao:virtual@82.88.114.91'),
            # If reverse is True - reverse domain and subdomain
            (('com.foo', 'f001.www', '/bar', 'http', '599', 'query=x&b=c', 'fregment', 'me:passwd', True),
                  'http://me:passwd@www.f001.foo.com:599/bar?query=x&b=c#fregment'),
            # Add square brackets when unsplit URL with IPv6 as domain
            (('2001:db8:11a3:9d7:1f34:8a2e:7a0:765d', '', '', 'http', '8080', '', '', ''),
                  'http://[2001:db8:11a3:9d7:1f34:8a2e:7a0:765d]:8080')]

        for surl, expected_result in expected_results:
            self.assertEqual(domainutils.unsplit_url(*surl), expected_result)


    def test_convert_str_to_uint(self):
        expected_results = [
            ('spam', 3768543861),
            ('foo.bar', 83460352)]
        for splain_str, expected_result in expected_results:
            self.assertEqual(domainutils.convert_str_to_uint(splain_str), expected_result)


    def test_url_to_keys(self):
        expected_results = [
            ('http://foo.com/abc', (377095192, 2899357301)),
            ('ftp://cartao:virtual@82.88.114.91', (1006547382, 3793389135)),
            ('www.112.124.72.92.com', (3032058714, 3404868746))]

        for url, expected_result in expected_results:
            self.assertEqual(domainutils.url_to_keys(url), expected_result)


    def test_convert_path_to_hash(self):
        expected_results = [
            ('/', '/'),
            ('', ''),
            ('/abc/', '/900150983cd2/'),
            ('/abc/cde', '/900150983cd2/a256e6b336af')]

        for spath, expected_result in expected_results:
            self.assertEqual(domainutils.convert_path_to_hash(spath), expected_result)

        # Path string must be valid
        self.assertRaises(domainutils.PathSegmentFormatError, domainutils.convert_path_to_hash, 'abc')


    def test_looks_like_ip(self):
        expected_results = [
            ('1.2.3.4', True),
            ('[2001:db8:11a3:9d7:1f34:8a2e:7a0:765d]', True),
            ('2001:db8:11a3:9d7:1f34:8a2e:7a0:765d', True),
            ('213.172.0x1f.13', True),  # 213.172.31.13
            ('0x7f000001', True),  # 127.0.0.1
            ('1113343453', True),  # 66.92.69.221
            ('[::ffff:192.0.2.0x80]', True),
            ('::ffff:192.0.2.0x80', True),
            ('[281473902969472]', True),
            ('[0xffffc0000280]', True),

            ('boo.foo', False),
            ('1.2.3.4.256', False),
            ('[::ffff:192.0.2.0xYZ]', False),
            ('boo[:]foo', False)]

        for dom_str, expected_result in expected_results:
            self.assertEqual(domainutils.looks_like_ip(dom_str),
                             expected_result)


if __name__ == "__main__":
    unittest.main()
