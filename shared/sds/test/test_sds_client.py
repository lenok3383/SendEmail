"""Unit tests for shared.sds.__init__.py module.

:Status: $Id: //prod/main/_is/shared/python/sds/test/test_sds_client.py#2 $
:Author: aianushe
:Last Modified By: $Author: djones $
"""

import cStringIO
import inspect
import json
import logging
import os
import pprint
import pycurl
import random
import sys
import time
import unittest2 as unittest

import shared.sds
from shared.sds import SDS_STATUS_URL

from pycurl_mockup import CurlMultiMockup
from pycurl_mockup import CurlMockup

CAT_VERSION = 1
SDS_HOST = 'v1.sds.cisco.com'

# X-SDS-Categories-Version HTTP header
SDS_CAT_VERSION_HEADER = 'x-sds-categories-version'
# Template for Web category API request
SDS_WEB_CATEGORY_URL = '/score/webcat/json?url=%s'

random.seed()

def _get_categorization_uri(item):
    """Get categorization URI."""
    return 'https://%s%s' % (SDS_HOST, SDS_WEB_CATEGORY_URL % (item,))


class TestSDSClient(unittest.TestCase):

    """Test SDSClient class."""

    def setUp(self):
        """Setup.

        Set SDSClient object, mock pycurl.Curl and pycurl.CurlMulti,
        set test_data_dir.
        """
        self.old_Curl = pycurl.Curl
        pycurl.Curl = CurlMockup

        self.old_CurlMulti = pycurl.CurlMulti
        pycurl.CurlMulti = CurlMultiMockup

        self._client = shared.sds.SDSClient(SDS_HOST, None, 'aianushe',
                                 'DevLookupTool', num_connections=3,
                                 log=logging.getLogger())

        self_file = inspect.getsourcefile(self.__class__)
        self_dir = os.path.dirname(os.path.abspath(self_file))
        self.test_data_dir = os.path.join(self_dir, 'data')

    def tearDown(self):
        """Restore pycurl.Curl and pycurl.CurlMulti."""
        pycurl.Curl = self.old_Curl
        pycurl.CurlMulti = self.old_CurlMulti

    def _get_status_url(self):
        """Get status URL."""
        return 'https://%s%s' % (SDS_HOST, SDS_STATUS_URL)

    def test_get_server_status(self):
        """Test get_server_status() method."""
        server_status_mockup_path = os.path.join(self.test_data_dir,
                                                 'server_status_mockup.json')
        with open(server_status_mockup_path, 'r') as data_file:
            server_status_mockup = json.load(data_file)
        CurlMockup.set_data({self._get_status_url() : server_status_mockup})

        self.assertEquals(self._client.get_server_status(), 'OK')

    def test_query(self):
        """Test query() method."""
        server_status_mockup_path = os.path.join(self.test_data_dir,
                                                 'server_status_mockup.json')
        with open(server_status_mockup_path, 'r') as data_file:
            server_status_mockup = json.load(data_file)
        CurlMockup.set_data({self._get_status_url() : server_status_mockup})

        self.assertEquals(self._client.query(SDS_STATUS_URL), 'OK')

        """Test query() method with more than 1 item in payload."""
        category_mockup_path = os.path.join(self.test_data_dir,
                                            'category_mockup.json')
        with open(category_mockup_path, 'r') as data_file:
            category_mockup = json.load(data_file)

        category_mockup_expected_path = os.path.join(self.test_data_dir,
                                               'category_mockup_expected.json')
        with open(category_mockup_expected_path, 'r') as data_file:
            category_mockup_expected = json.load(data_file)

        curl_mock_dict = {}
        for key, value in category_mockup.iteritems():
            curl_mock_dict[_get_categorization_uri(key)] = value
        CurlMockup.set_data(curl_mock_dict)

        headers = {SDS_CAT_VERSION_HEADER: 'v%s' % (CAT_VERSION,)}
        result = self._client.query(SDS_WEB_CATEGORY_URL,
                                    payload=category_mockup.keys(),
                                    request_headers=headers)
        self.assertEquals(result, category_mockup_expected)

        """Test query() method with 1 item in payload."""
        key = category_mockup.keys()[0]
        value = category_mockup[key]
        curl_mock_dict = {_get_categorization_uri(key) : value}
        CurlMockup.set_data(curl_mock_dict)

        headers = {SDS_CAT_VERSION_HEADER: 'v%s' % (CAT_VERSION,)}
        result = self._client.query(SDS_WEB_CATEGORY_URL,
                                    payload=key,
                                    request_headers=headers)
        expected = category_mockup_expected[key]

        with self.assertRaises(TypeError):
            self._client.query(SDS_WEB_CATEGORY_URL, {})

    def test_query_single(self):
        """Test query_single() method."""
        server_status_mockup_path = os.path.join(self.test_data_dir,
                                                 'server_status_mockup.json')
        with open(server_status_mockup_path, 'r') as data_file:
            server_status_mockup = json.load(data_file)
        CurlMockup.set_data({self._get_status_url() : server_status_mockup})
        self.assertEquals(self._client.query_single(SDS_STATUS_URL), 'OK')


class TestSDSMultiQuery(unittest.TestCase):

    """Test SDSMultiQuery class."""

    def setUp(self):
        """Setup.

        Set SDSMultiQuery object, mock pycurl.Curl and pycurl.CurlMulti,
        set test_data_dir.
        """
        self.old_Curl = pycurl.Curl
        pycurl.Curl = CurlMockup

        self.old_CurlMulti = pycurl.CurlMulti
        pycurl.CurlMulti = CurlMultiMockup

        self._client = shared.sds.SDSMultiQuery(SDS_HOST, None, 'aianushe',
                                     'DevLookupTool',
                                     num_connections=3, timeout=10,
                                     log=logging.getLogger())

        self_file = inspect.getsourcefile(self.__class__)
        self_dir = os.path.dirname(os.path.abspath(self_file))
        self.test_data_dir = os.path.join(self_dir, 'data')

    def tearDown(self):
        """Restore pycurl.Curl and pycurl.CurlMulti."""
        pycurl.Curl = self.old_Curl
        pycurl.CurlMulti = self.old_CurlMulti

    def test_run(self):
        """Test run() method."""
        curl_mock_dict = {}

        category_mockup_path = os.path.join(self.test_data_dir,
                                            'category_mockup.json')
        with open(category_mockup_path, 'r') as data_file:
            category_mockup = json.load(data_file)

        category_mockup_expected_path = os.path.join(self.test_data_dir,
                                            'category_mockup_expected.json')
        with open(category_mockup_expected_path, 'r') as data_file:
            category_mockup_expected = json.load(data_file)

        for key, value in category_mockup.iteritems():
            curl_mock_dict[_get_categorization_uri(key)] = value
        CurlMockup.set_data(curl_mock_dict)

        headers = {SDS_CAT_VERSION_HEADER: 'v%s' % (CAT_VERSION,)}
        result = self._client.run(SDS_WEB_CATEGORY_URL,
                                  payload=category_mockup.keys(),
                                  request_headers=headers)
        self.assertEquals(result, category_mockup_expected)


class TestSDSFunctions(unittest.TestCase):

    """Test sds module functions."""

    def test_parse_response_headers(self):
        """Test _parse_response_headers() method."""
        raw_headers = cStringIO.StringIO()
        raw_headers.write('HTTP/1.1 200 OK\r\n'
                          'Server: nginx/0.8.54\r\n'
                          'Date: Wed, 17 Jul 2013 10:14:21 GMT\r\n'
                          'Content-Type: text/plain\r\n'
                          'Connection: keep-alive\r\n'
                          'Content-Length: 2\r\n\r\n')
        resp_header_dict = {}
        raw_headers.seek(0)
        content_type = shared.sds._parse_response_headers(raw_headers,
            resp_header_dict)

        self.assertEquals(content_type, 'text/plain')

        expected_resp_header_dict = {'date': 'wed, 17 jul 2013 10:14:21 gmt',
                                     'connection': 'keep-alive',
                                     'content-type': 'text/plain',
                                     'content-length': '2',
                                     'server': 'nginx/0.8.54'}
        self.assertEquals(resp_header_dict, expected_resp_header_dict)

class TestSDSResponseCache(unittest.TestCase):

    def setUp(self):
        fmt = '%(asctime)s [%(filename)s,%(lineno)d] [%(name)s] %(levelname)s %(message)s'
        logfn = sys.argv[0].replace('.py','.log')
        logging.basicConfig(filename=logfn, level=logging.ERROR, format=fmt)
        self.log = logging.getLogger(self.__class__.__name__)

    def test01_wildcard_pattern_matching(self):
        """Splat-pattern matching tests."""

        patterns = { # {pattern: id}
            'foo.com': 1,
            '*.foo.com/*': 2,
            'sub1.foo.com/a/*': 3,
            'sub1.foo.com/a/b/infect.html': 4,
            '*.img.foo.com/a/b/infect.html/*': 5,
        }
        testdata = { # {(subdomain, domain, path): pattern id expected to hit}
            ('', 'foo.com', ''): 1,
            ('a.b.c', 'foo.com', '/some/path.txt'): 2,
            ('sub1', 'foo.com', '/a/b/infect.html'): 4,
            ('sub2', 'foo.com', '/a/b/infect.html'): 2,
            ('a.sub1', 'FOO.COM', '/a/b/infect.html'): 2,
            ('SUB1', 'FOO.COM', '/A/B/C.TXT'): 3,
            ('sub1', 'foo2.com', '/a/b/c.txt'): None,
            ('norcal.img', 'foo.com', '/a/b/infect.html?some=junk'): 5,
            ('', '', ''): None, # Invalid, domain required.
        }
        for (subdom, dom, path), expect_id in testdata.iteritems():
            pat = shared.sds.SDSResponseCache.wildcard_url_pattern_match(
                patterns.keys(), dom, subdom, path, self.log)
            self.assertEquals(patterns.get(pat), expect_id)

    def test02_cache_set(self):
        cache = shared.sds.SDSResponseCache(maxsize=10, logger=self.log)
        cache.set('a.com', -1.111, '*.a.com/*', 10)
        cache.set('a.com', -1.111, '*.a.com/*', 10) # Duplicates only reset ttl timestamp
        cache.set('b.com', -1.111, '*.b.com/*', 10)
        cache.set('c.com', -1.111, '*.c.com/*', 10)
        cache.set('foo.com', -1.123, '*.foo.com/*', 10)
        cache.set('foo.com', -4.555, '*.sub1.foo.com/a/b/*', 10)
        cache.set('foo.com', -7.666, 'sub2.foo.com/a/b/infect.html', 10)
        self.assertEquals(str(cache),
            '<SDSResponseCache size=6 maxsize=10 oldest=a.com newest=foo.com hits=0 misses=0>')
        self.assertEquals(cache.domain_list(),
            [('foo.com', 3), ('c.com', 1), ('b.com', 1), ('a.com', 1)])

    def test03_cache_maxsize(self):
        cache = shared.sds.SDSResponseCache(maxsize=3, logger=self.log)
        cache.set('a.com', -1.111, '*.a.com/*', 10)
        cache.set('b.com', -1.111, '*.b.com/*', 10)
        cache.set('c.com', -1.111, '*.c.com/*', 10)
        cache.set('d.com', -1.111, '*.d.com/*', 10)
        self.assertEquals(cache.domain_list(),
            [('d.com', 1), ('c.com', 1), ('b.com', 1)])

        cache = shared.sds.SDSResponseCache(maxsize=3, logger=self.log)
        cache.set('a.com', -1.111, '*.a.com/*', 10)
        cache.set('b.com', -1.111, '*.b.com/*', 10)
        cache.set('c.com', -1.111, '*.c.com/*', 10)
        cache.set('d.com', -1.111, '*.d.com/*', 10)
        cache.set('d.com', -1.111, 'two', 10)
        self.assertEquals(cache.domain_list(),
            [('d.com', 2), ('c.com', 1)])

        cache = shared.sds.SDSResponseCache(maxsize=3, logger=self.log)
        cache.set('a.com', -1.111, '*.a.com/*', 10)
        cache.set('d.com', -1.111, '*.d.com/*', 10)
        cache.set('d.com', -1.111, 'two', 10)
        cache.set('d.com', -1.111, 'three', 10)
        cache.set('d.com', -1.111, 'four', 10)
        self.assertEquals(cache.domain_list(), [])

    def test04_cache_remove(self):
        cache = shared.sds.SDSResponseCache(maxsize=10)

        cache.set('a.com', -1.111, '*.a.com/*', 10)
        cache.set('b.com', -1.111, '*.b.com/*', 10)
        cache.set('c.com', -1.111, '*.c.com/*', 10)
        cache.set('d.com', -1.111, '*.d.com/*', 10)
        cache.remove('b.com')
        cache.remove('c.com')
        self.assertEquals(cache.domain_list(),
            [('d.com', 1), ('a.com', 1)])

        cache.remove('d.com')
        cache.set('e.com', -1.111, '*.e.com/*', 10)
        self.assertEquals(cache.domain_list(),
            [('e.com', 1), ('a.com', 1)])

        cache.remove('e.com')
        cache.remove('a.com')
        self.assertEquals(cache.domain_list(), [])

        cache.set('a.com', -1.111, '*.a.com/*', 10)
        cache.remove('z.com')  # Invalid domains are ignored.
        cache.remove('')  # Invalid domains are ignored.
        self.assertEquals(cache.domain_list(),
            [('a.com', 1)])

        cache = shared.sds.SDSResponseCache(maxsize=10)
        cache.set('a.com', -1.111, '*.a.com/*', 10)
        cache.set('b.com', -1.111, '*.b.com/*', 10)
        cache.set('c.com', -1.111, '*.c.com/*', 10)
        cache.remove('c.com', delete_node=False) # Still exists but not in linked list.
        cache.remove('a.com', delete_node=True)
        self.assertEquals(cache.domain_list(),
            [('b.com', 1)])
        self.assertEquals(str(cache),
            '<SDSResponseCache size=1 maxsize=10 oldest=b.com newest=b.com hits=0 misses=0>')
        self.assertEquals(sorted(cache._cache.keys()),
            ['b.com', 'c.com'])

    def test05a_cache_get_lru(self):
        cache = shared.sds.SDSResponseCache(maxsize=7)
        cache.set('a.com', -1.111, '*.a.com/*', 10)
        cache.set('b.com', -1.111, '*.b.com/*', 10)
        cache.set('c.com', -1.111, '*.c.com/*', 10)
        cache.set('d.com', -1.111, '*.d.com/*', 10)
        cache.set('foo.com', -1.123, '*.foo.com/z*', 10)
        cache.set('foo.com', -4.555, '*.sub1.foo.com/a/b/*', 10)
        cache.set('foo.com', -7.666, 'sub2.foo.com/a/b/infect.html', 10)
        self.assertEquals(str(cache),
            '<SDSResponseCache size=7 maxsize=7 oldest=a.com newest=foo.com hits=0 misses=0>')
        self.assertEquals(cache.domain_list(),
            [('foo.com', 3), ('d.com', 1), ('c.com', 1), ('b.com', 1), ('a.com', 1)])

        # Affects of lookups on LRU.
        cache.get('c.com', 'img1', '/a/b/c/d.jpg') # Hit. Moves to top.
        cache.get('a.com', 'img1', '/a/b/c/d.jpg') # Hit. Moves to top.
        cache.get('z.com', '', '') # Miss, shouldnt affect anything.
        cache.get('foo.com', 'img1', '/a/yyy/d.jpg') # Miss. Still moves to top.
        self.assertEquals(str(cache),
            '<SDSResponseCache size=7 maxsize=7 oldest=b.com newest=foo.com hits=2 misses=2>')
        self.assertEquals(cache.domain_list(),
            [('foo.com', 3), ('a.com', 1), ('c.com', 1), ('d.com', 1), ('b.com', 1)])

        # Set lops off oldest.
        cache.set('e.com', -1.111, '*.e.com/*', 10)
        self.assertEquals(cache.domain_list(),
            [('e.com', 1), ('foo.com', 3), ('a.com', 1), ('c.com', 1), ('d.com', 1)])

    def test05b_cache_get_values(self):
        cache = shared.sds.SDSResponseCache(maxsize=10)
        cache.set('a.com', -1.111, '*.a.com/*', 10)
        cache.set('foo.com', -1.123, '*.foo.com/z*', 10)
        cache.set('foo.com', -4.555, '*.sub1.foo.com/a/b/*', 10)
        cache.set('foo.com', -7.666, 'sub2.foo.com/a/b/infect.html', 10)
        self.assertEquals(str(cache),
            '<SDSResponseCache size=4 maxsize=10 oldest=a.com newest=foo.com hits=0 misses=0>')
        self.assertEquals(cache.domain_list(),
            [('foo.com', 3), ('a.com', 1)])

        # Miss has no affect on system but to increment miss counter.
        score = cache.get('z.com', '', '')
        self.assertEquals(score, None)
        score = cache.get('foo.com', 'img1', '/a/yyy/d.jpg')
        self.assertEquals(score, None)
        self.assertEquals(str(cache),
            '<SDSResponseCache size=4 maxsize=10 oldest=a.com newest=foo.com hits=0 misses=2>')

        # Hits.
        score = cache.get('foo.com', 'img1.sub1', '/a/b/d.jpg')
        self.assertEquals(score, -4.555)
        score = cache.get('a.com', '', '')
        self.assertEquals(score, -1.111)
        self.assertEquals(str(cache),
            '<SDSResponseCache size=4 maxsize=10 oldest=foo.com newest=a.com hits=2 misses=2>')

    def test06_cache_ttl(self):
        TTL = 1.0
        cache = shared.sds.SDSResponseCache(maxsize=10)
        cache.set('a.com', -1.111, '*.a.com/*', TTL)
        cache.set('foo.com', -1.123, '*.foo.com/z*', TTL + 10)
        cache.set('foo.com', -4.555, '*.sub1.foo.com/a/b/*', TTL)
        cache.set('foo.com', -7.666, 'sub2.foo.com/a/b/infect.html', TTL)
        self.assertEquals(str(cache),
            '<SDSResponseCache size=4 maxsize=10 oldest=a.com newest=foo.com hits=0 misses=0>')
        self.assertEquals(cache.domain_list(),
            [('foo.com', 3), ('a.com', 1)])

        score = cache.get('foo.com', 'img1.sub1', '/a/b/d.jpg')
        self.assertEquals(score, -4.555)
        time.sleep(TTL + 0.5)
        score = cache.get('foo.com', 'img1.sub1', '/a/b/d.jpg')
        self.assertEquals(score, None)
        # a.com still there even tho expired because not looked up yet. Lazy expiration.
        # No worries, its LRU.
        self.assertEquals(cache.domain_list(),
            [('foo.com', 1), ('a.com', 0)])

        score = cache.get('a.com', 'img1.sub1', '/a/b/d.jpg')
        self.assertEquals(score, None)
        self.assertEquals(str(cache),
            '<SDSResponseCache size=1 maxsize=10 oldest=foo.com newest=foo.com hits=1 misses=2>')
        self.assertEquals(cache.domain_list(),
            [('foo.com', 1)]) # Now a.com is removed.

        # print cache; print cache.domain_list(); pprint.pprint(cache._cache)

    def test07_cache_performance(self):
        # Intel Xeon X5680 @ 3.33GHz 2 core Linux VM: 3.4095048904418945 sec
        # Confirm logging is not set to DEBUG.
        MAXSIZE = 100000
        INSERTS = MAXSIZE * 2
        LOOKUPS = MAXSIZE * 2
        TARGET_TIME = 9.0 # Operations expected to execute faster than this.

        TTL = 2.0
        t1 = time.time()

        cache = shared.sds.SDSResponseCache(maxsize=MAXSIZE)

        for i in range(1, INSERTS + 1):
            cache.set('%s.com' % i, -1.111, '*.%s.com/*' % i, TTL)
            cache.set('%s.com' % i, -2.222, '*.foo.%s.com/*' % i, TTL)
            cache.set('%s.com' % i, -4.444, 'foo.%s.com/a/b/c/d/infect.jpg*' % i, TTL)

        # Random lookups. Hits and misses.
        for i in range(1, LOOKUPS + 1):
            cache.get('%s.com' % random.randint(1, MAXSIZE * 2), '', '')

        took = time.time() - t1
        self.assertLessEqual(took, TARGET_TIME)

        # print cache; print cache.domain_list(); pprint.pprint(cache._cache)

if __name__ == '__main__':
    unittest.main()
