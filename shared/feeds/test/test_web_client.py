"""Feeds Web API client module tests.

:Status: $Id: //prod/main/_is/shared/python/feeds/test/test_web_client.py#6 $
:Authors: migoldsb
"""

import unittest2
from shared.testing.vmock import mockcontrol

import cStringIO
import random
import rfc822
from hashlib import md5

from shared.feeds import utils
from shared.feeds import web_client

TEST_USERNAME = 'testuser'
MOCK_TEST_HOST = 'fake.feedsweb.server'
LIVE_TEST_HOST = None

DEFAULT_REQ_HEADERS = {web_client.USERNAME_HEADER: TEST_USERNAME}
STALE_REQ_HEADERS = {web_client.USERNAME_HEADER: TEST_USERNAME,
                     'If-Modified-Since': 'Fri, 09 Dec 2011 20:17:15 GMT'}

MOCK_HTTP_REQUEST_PARAMS = {
    'get_all_feeds_status':
        {'args': (MOCK_TEST_HOST, '/all.json'),
         'kwargs': {'req_headers': DEFAULT_REQ_HEADERS,
                    'timeout': None}},

    'get_feed_status':
        {'args': (MOCK_TEST_HOST, '/test.json'),
         'kwargs': {'req_headers': DEFAULT_REQ_HEADERS,
                    'timeout': None}},

    'get_feed_file':
        {'args': (MOCK_TEST_HOST, '/test/data/test_file'),
         'kwargs': {'req_headers': DEFAULT_REQ_HEADERS,
                    'tmp_dir': None,
                    'timeout': None}},

    'get_bogus_feed_status':
        {'args': (MOCK_TEST_HOST, '/bogus_feed.json'),
         'kwargs': {'req_headers': DEFAULT_REQ_HEADERS,
                    'timeout': None}},

    'get_bogus_feed_file':
        {'args': (MOCK_TEST_HOST, '/test/data/bogus_file'),
         'kwargs': {'req_headers': DEFAULT_REQ_HEADERS,
                    'tmp_dir': None,
                    'timeout': None}},

    'get_stale_feed_file':
        {'args': (MOCK_TEST_HOST, '/test/data/test_file'),
         'kwargs': {'req_headers': STALE_REQ_HEADERS,
                    'tmp_dir': None,
                    'timeout': None}},

    'get_corrupt_feed_file':
        {'args': (MOCK_TEST_HOST, '/test/data/corrupt_file'),
         'kwargs': {'req_headers': DEFAULT_REQ_HEADERS,
                    'tmp_dir': None,
                    'timeout': None}},

    'server_error':
        {'args': (MOCK_TEST_HOST, '/test/data/bat_country'),
         'kwargs': {'req_headers': DEFAULT_REQ_HEADERS,
                    'tmp_dir': None,
                    'timeout': None}},
}

MOCK_HTTP_REQUEST_RETURN = {
    'get_all_feeds_status':
        (200,
         dict([('status', '200 OK'),
          ('date', 'Fri, 09 Dec 2011 20:40:17 GMT'),
          ('content-type', 'application/json')]),
         cStringIO.StringIO('{"feeds":{"test":{"status":'
         '{"last_successful_update":1323461841,'
         '"next_update_attempt":1323466742,'
         '"last_update_attempt":1323463142}}},"db_unix_ts":1323463217}')),

    'get_feed_status':
        (200,
         dict([('status', '200 OK'),
          ('date', 'Fri, 09 Dec 2011 20:43:07 GMT'),
          ('content-type', 'application/json')]),
         cStringIO.StringIO('{"files":{"test_file":{"mtime":1323461835,'
         '"uri":"http:\\/\\/feeds.fake.url",'
         '"md5":"07aa24d4ee5d5d29fbaf360896713947",'
         '"relative_path":"test\\/archive\\/'
         'test_file.20111209-201714_UTC"}},"db_unix_ts":1323463387}')),

    'get_feed_file':
        (200,
         dict([('status', '200 OK'),
          ('last-modified', 'Fri, 09 Dec 2011 20:17:15 GMT'),
          ('content-length', '20'),
          ('content-location', 'http://feeds.fake.url/test/archive/'
                               'test_file.20111209-201714_UTC'),
          ('date', 'Fri, 09 Dec 2011 20:40:17 GMT'),
          ('content-md5', '07aa24d4ee5d5d29fbaf360896713947')]),
         cStringIO.StringIO('This is a test file!')),

    'get_bogus_feed_status':
        (404,
         dict([('status', '404 Not Found'),
          ('content-type', 'application/json')]),
         cStringIO.StringIO('"404 Not Found\\nNot Found\\nInvalid Feed Error: '
         '\'Exception: Feed bogus_feed does not exist.\'\\n"\n')),

    'get_bogus_feed_file':
        (404,
         dict([('status', '404 Not Found'),
          ('content-type', 'text/html')]),
         cStringIO.StringIO('<html>\n  <head>\n    '
         '<title>404 Not Found</title>\n  </head>\n  <body>\n    '
         '<h1>Not Found</h1>\n    '
         "File 'bogus_file' does not exist in feed 'catbayes'\n  "
         '</body>\n</html>\n')),

    'get_stale_feed_file':
        (304,
         dict([('status', '304 Not Modified')]),
         cStringIO.StringIO('')),

    'server_error':
        (500,
         dict([('status', '500 Internal Server Error'),
               ('content-type', 'text/html')]),
         cStringIO.StringIO('<html><head>\n'
                 '<title>500 Internal Server Error</title>\n</head><body>\n'
                 '<h1>Internal Server Error</h1>\n</body></html>\n')),
}

EXPECTED = {
    'get_all_feeds_status':
        {'feeds':
            {'test':
                {'status':
                    {'last_successful_update': 1323461841,
                     'next_update_attempt': 1323466742,
                     'last_update_attempt': 1323463142}
                }
            },
         'db_unix_ts': 1323463217},

    'get_feed_status':
        {'files':
            {'test_file':
                {'md5': '07aa24d4ee5d5d29fbaf360896713947',
                 'mtime': 1323461835,
                 'relative_path': 'test/archive/test_file.20111209-201714_UTC',
                 'uri': 'http://feeds.fake.url'}
            },
         'db_unix_ts': 1323463387},

    'get_feed_file': cStringIO.StringIO('This is a test file!'),

    'get_all_feed_names': ['test'],

    'get_feed_filenames': ['test_file'],

    'get_feed_file_meta':
        {'md5': '07aa24d4ee5d5d29fbaf360896713947',
         'mtime': 1323461835,
         'relative_path': 'test/archive/test_file.20111209-201714_UTC',
         'uri': 'http://feeds.fake.url'},
}


def live_test():
    """Performs a live test by calling all the API methods:
    get_all_feeds_status, get_feed_status, and get_feed_file.  Fetches the
    status of a random feed, then fetches a random file from that feed."""

    # Get all feeds' status.
    fwc = web_client.FeedsWebClient(LIVE_TEST_HOST, TEST_USERNAME)
    all_feeds = fwc.get_all_feed_names()
    print 'Got all feed names: %s' % (' '.join(all_feeds),)

    # Get random feed's files.
    feed_files = []
    while not feed_files:
        feed = random.choice(all_feeds)
        feed_files = fwc.get_feed_filenames(feed)
    print "Feed '%s' has files: '%s'" % (feed, "', '".join(feed_files))

    # Get a random file.
    feed_file = random.choice(feed_files)
    file_meta = fwc.get_feed_file_meta(feed, feed_file)
    file_data = fwc.get_feed_file(feed, feed_file)
    file_md5 = md5(file_data.read()).hexdigest()
    meta_md5 = file_meta['md5']
    if file_md5 == meta_md5:
        print "File '%s' MD5 matches: %s" % (feed_file, meta_md5)
    else:
        print "Error: File '%s' MD5 %s does not match %s" % (feed_file,
                file_md5, meta_md5)

    # Request the same file again.  Should return None, assuming it hasn't been
    # updated in the last few seconds.
    file_data = fwc.get_feed_file(feed, feed_file)
    if not file_data:
        print 'Duplicate request returned None.'


class FeedsWebAPIClientTest(unittest2.TestCase):

    """Feeds Web API Client unit tests."""

    def setUp(self):
        self.mc = mockcontrol.MockControl()
        self.mock_http_request = self.mc.mock_method(utils, 'http_request')
        self.fwc = web_client.FeedsWebClient(MOCK_TEST_HOST, TEST_USERNAME)

    def tearDown(self):
        self.mc.tear_down()

    def setup_http_request_mock(self, test_name):
        """Sets up the http_request mock call(s) for the given test."""

        args = MOCK_HTTP_REQUEST_PARAMS[test_name]['args']
        kwargs = MOCK_HTTP_REQUEST_PARAMS[test_name].get('kwargs', {})
        ret = MOCK_HTTP_REQUEST_RETURN[test_name]
        # Don't ask me, I have no idea:
        ret[2].seek(0)

        self.mock_http_request(*args, **kwargs).returns(ret)

    def test_get_all_feeds_status(self):
        """Tests that get_all_feeds_status returns the all-feeds status dict.
        """

        test = 'get_all_feeds_status'
        self.setup_http_request_mock(test)
        self.mc.replay()

        result = self.fwc.get_all_feeds_status()
        self.assertEqual(EXPECTED[test], result)

        self.mc.verify()

    def test_get_feed_status(self):
        """Tests that get_feed_status returns the feed status dict."""

        test = 'get_feed_status'
        self.setup_http_request_mock(test)
        self.mc.replay()

        result = self.fwc.get_feed_status('test')
        self.assertEqual(EXPECTED[test], result)

        self.mc.verify()

    def test_get_feed_file(self):
        """Tests that get_feed_file returns the file contents."""

        test = 'get_feed_file'
        self.setup_http_request_mock(test)
        self.mc.replay()

        result = self.fwc.get_feed_file('test', 'test_file')
        self.assertEqual(EXPECTED[test].read(), result.read())

        self.mc.verify()

    def test_get_bogus_feed_status(self):
        """Tests that trying to get a nonexistent feed raises a
        NoSuchFeedError."""

        test = 'get_bogus_feed_status'
        self.setup_http_request_mock(test)
        self.mc.replay()

        self.assertRaises(web_client.NoSuchFeedError,
                          self.fwc.get_feed_status,
                          'bogus_feed')

        self.mc.verify()

    def test_get_bogus_feed_file(self):
        """Tests that trying to get a nonexistent file raises a
        NoSuchFileInFeedError."""

        test = 'get_bogus_feed_file'
        self.setup_http_request_mock(test)
        self.mc.replay()

        self.assertRaises(web_client.NoSuchFileInFeedError,
                          self.fwc.get_feed_file,
                          'test', 'bogus_file')

        self.mc.verify()

    def test_get_stale_feed_file(self):
        """Tests that successive calls to get_feed_file for the same file
        return None when the file has not been modified since it was last
        fetched."""

        self.setup_http_request_mock('get_feed_file')
        self.setup_http_request_mock('get_stale_feed_file')
        self.mc.replay()

        result = self.fwc.get_feed_file('test', 'test_file')
        EXPECTED['get_feed_file'].seek(0)
        self.assertEqual(EXPECTED['get_feed_file'].read(), result.read())
        result = self.fwc.get_feed_file('test', 'test_file')
        self.assertIsNone(result)

        self.mc.verify()

    def test_get_corrupt_feed_file(self):
        """Tests that receiving a file whose MD5 does not match (i.e. retrying
        the download doesn't fix it) raises an InvalidMD5Error."""

        test = 'get_corrupt_feed_file'
        args = MOCK_HTTP_REQUEST_PARAMS[test]['args']
        kwargs = MOCK_HTTP_REQUEST_PARAMS[test].get('kwargs', {})
        self.mock_http_request(*args, **kwargs).raises(
                utils.InvalidMD5Error('1234', '5678'))
        self.mc.replay()

        self.assertRaises(web_client.InvalidMD5Error,
                          self.fwc.get_feed_file,
                          'test', 'corrupt_file')

        self.mc.verify()

    def test_server_error(self):
        """Tests that unexpected server behavior (response codes 5xx) raises a
        FeedsWebAPIServerError."""

        test = 'server_error'
        self.setup_http_request_mock(test)
        self.mc.replay()

        self.assertRaises(web_client.FeedsWebAPIServerError,
                          self.fwc.get_feed_file,
                          'test', 'bat_country')

        self.mc.verify()

    def test_utility_functions(self):
        """Tests the utility functions get_all_feed_names, get_feed_filenames,
        and get_feed_file_meta.  This will also implicitly test caching."""

        self.setup_http_request_mock('get_all_feeds_status')
        self.setup_http_request_mock('get_feed_status')
        self.mc.replay()

        result = self.fwc.get_all_feed_names()
        self.assertEqual(result, EXPECTED['get_all_feed_names'])

        result = self.fwc.get_feed_filenames('test')
        self.assertEqual(result, EXPECTED['get_feed_filenames'])

        result = self.fwc.get_feed_file_meta('test', 'test_file')
        self.assertEqual(result, EXPECTED['get_feed_file_meta'])

        self.mc.verify()

    def test_set_fetched_feeds_state_copy(self):
        """Test if the client set a copy of the state."""
        c = web_client.FeedsWebClient('unknown_host', 'test_client')

        tmp_status = {'1': 1}
        c.set_fetched_feeds_state(tmp_status)
        tmp_status['2'] = 2
        self.assertNotEquals(tmp_status, c.get_fetched_feeds_state())
        self.assertEquals({'1': 1}, c.get_fetched_feeds_state())

    def test_get_fetched_feeds_state_copy(self):
        """Test if client returns a copy of the state."""
        c = web_client.FeedsWebClient('unknown_host', 'test_client')

        c.set_fetched_feeds_state({'1': 1})
        feeds_state = c.get_fetched_feeds_state()
        feeds_state['2'] = 2
        # Make sure feeds returns a copy of the status.
        self.assertNotEquals(feeds_state, c.get_fetched_feeds_state())
        self.assertEquals({'1': 1}, c.get_fetched_feeds_state())


if __name__ == '__main__':
    if LIVE_TEST_HOST:
        print 'Running live test with host: %s' % (LIVE_TEST_HOST,)
        live_test()

    unittest2.main()

