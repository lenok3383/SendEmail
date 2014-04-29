"""Feeds Web API client module for use by products using the Python 2.6 shared
libraries.  See feeds_webapi package documentation for more details.

:Status: $Id: //prod/main/_is/shared/python/feeds/web_client.py#7 $
:Authors: migoldsb
"""

import email.utils
import json
import urlparse

from shared.feeds import utils
from shared.util import lrucache

# Size of the LRU time cache.
CACHE_SIZE = 256
# How long to cache updates from the Feeds server (seconds).
CACHE_TTL = 10
# Key to store all-feeds status under.
ALL_FEEDS_KEY = '__all'
# The HTTP header containing the Feeds client username.
USERNAME_HEADER = 'X-Feeds-Client-Username'

# MD5 read chunk size.
FILE_CHUNK_SIZE = 1024 * 1024


class FeedsWebClientError(Exception):
    """Base class for all FeedsWebClient exceptions."""
    def __init__(self):
        pass


class NoSuchFeedError(FeedsWebClientError):
    """Raised when a nonexistent feed is requested."""
    def __init__(self, feed_name):
        self.args = ["Feed '%s' does not exist" % (feed_name,)]


class NoSuchFileInFeedError(FeedsWebClientError):
    """Raised when the requested filename doesn't exist in the feed."""
    def __init__(self, feed_name, filename):
        self.args = ["File '%s' does not exist in feed '%s'"
                     % (filename, feed_name)]


class InvalidMD5Error(FeedsWebClientError):
    """Raised when a file's MD5 doesn't match."""
    def __init__(self, e):
        self.args = e.args


class FeedsWebAPIServerError(FeedsWebClientError):
    """Raised for any unexpected server errors (usually 5xx)."""
    def __init__(self, msg):
        self.args = ['Feeds Web API server error: %s' % (msg,)]


class FeedsWebClient(object):

    """Feeds Web Client class.

    Acts as an intermediary between products using Python 2.6 and the fastrpc-
    based Python 2.4 Feeds API, by way of a Feeds Web API server.

    Responses are cached for a few seconds to prevent rapid redundant requests.
    """

    def __init__(self, feeds_host, client_username, feeds_base_path='/',
                 tmp_file_dir=None, http_timeout=None):
        """Constructor.

        :param feeds_host: The Feeds Web API server host[:port].
        :param client_username: The username to supply to the Feeds Web API
            server, for logging.
        :param feeds_base_path: The base path for feed requests; requests will
            be in the form 'feeds_base_path/feed_name/data/file'.
        :param tmp_file_dir: Where to store the files fetched from the Feeds
            server.  Defaults to '/tmp' or $TMPDIR if None.
        :param http_timeout: Timeout (in seconds) for HTTP connection sockets.
            Default is no timeout (wait forever).
        """

        self.feeds_host = feeds_host
        self.client_username = client_username
        self.feeds_base_path = feeds_base_path
        self.tmp_file_dir = tmp_file_dir
        self.http_timeout = http_timeout

        self.__default_req_headers = {USERNAME_HEADER: client_username}

        self.__cache = lrucache.LRUTimeCache(CACHE_SIZE, CACHE_TTL)
        self.__last_fetched = {}

        self.__GET_STATUS_TEMPLATE = urlparse.urljoin(self.feeds_base_path,
                '%(feed_name)s.json')
        self.__GET_PROVIDER_INFO_TEMPLATE = urlparse.urljoin(self.feeds_base_path,
                '%(feed_name)s_info.json')
        self.__GET_FILE_TEMPLATE = urlparse.urljoin(self.feeds_base_path,
                '%(feed_name)s/data/%(filename)s')

    def __get_status(self, feed_name):
        """Sends feed status request to server."""

        try:
            return self.__cache.get(feed_name)
        except KeyError:
            pass

        if feed_name == ALL_FEEDS_KEY:
            feed_name = 'all'

        url = self.__GET_STATUS_TEMPLATE % {'feed_name': feed_name}
        headers = dict(self.__default_req_headers)
        status, _, body = utils.http_request(self.feeds_host, url,
                req_headers=headers, timeout=self.http_timeout)
        if status == 404:
            raise NoSuchFeedError(feed_name)
        if not 200 <= status <= 299:
            raise FeedsWebAPIServerError(body.read())

        feed_status = json.loads(body.read())
        self.__cache.put(feed_name, feed_status)

        return feed_status

    def _get_provider_info(self, feed_name):
        """Sends feeds provider info to server"""

        url = self.__GET_PROVIDER_INFO_TEMPLATE % {'feed_name': feed_name}
        headers = dict(self.__default_req_headers)
        status, _, body = utils.http_request(self.feeds_host, url,
                req_headers=headers, timeout=self.http_timeout)
        if status == 404:
            raise NoSuchFeedError(feed_name)
        if not 200 <= status <= 299:
            raise FeedsWebAPIServerError(body.read())

        feed_provider_info = json.loads(body.read())
        self.__cache.put(feed_name, feed_provider_info)

        return feed_provider_info

    def get_all_feeds_status(self):
        """Passthrough to the Feeds client method of the same name.

        Requests '/all.json'.  The response should be identical to
        feeds_client.get_all_feed_status(), but in JSON.
        """

        return self.__get_status(ALL_FEEDS_KEY)

    def get_provider_info(self, feed_name):
        """Passthrough to the Feeds client method of the same name.

        Requests '/feed_name_info.json'.
        """

        return self._get_provider_info(feed_name)

    def get_feed_status(self, feed_name):
        """Passthrough to the Feeds client method of the same name.

        Requests '/feed_name.json'.  The response should be identical to
        feeds_client.get_feed_status('feed_name'), but in JSON.
        """

        return self.__get_status(feed_name)

    def get_all_feed_names(self):
        """Utility function.  Returns list of all feed names."""

        status = self.get_all_feeds_status()
        return sorted(status['feeds'].keys())

    def get_feed_filenames(self, feed_name):
        """Utility function.  Returns the filenames belonging to a feed."""

        status = self.get_feed_status(feed_name)
        return sorted(status['files'].keys())

    def get_feed_file_meta(self, feed_name, filename):
        """Fetch the named feed file's metadata."""

        status = self.get_feed_status(feed_name)
        if filename not in status['files']:
            raise NoSuchFileInFeedError(feed_name, filename)

        return status['files'][filename]

    def get_fetched_feeds_state(self):
        """Take a snapshot of the last fetched feeds state."""
        return self.__last_fetched.copy()

    def set_fetched_feeds_state(self, state):
        """Set user defined feeds state."""
        self.__last_fetched = state.copy()

    def get_feed_file(self, feed_name, filename, force_fetch=False):
        """Fetch a feed file.  The mtime of the file is saved when the file is
        fetched, and subsequent requests for the same file will only fetch the
        file if it has been changed since that time, effectively caching it.
        This can be overridden with force_fetch; otherwise, returns None if the
        requested file has not been modified since it was last fetched.

        Returns the feed file contents as a file-like object (either a StringIO
        or a NamedTemporaryFile, depending on its size).  Assume this file is
        ephemeral and will disappear forever when it is closed.

        Raises an InvalidMD5Error if the fetched file's calculated MD5 does not
        match the MD5 in its metadata.  Note that this will only occur after
        repeated attempts to redownload the file, so if this happens, it likely
        means an error in the metadata rather than a corrupt file.

        :param feed_name: Feed to which the file belongs.
        :param filename: File to be fetched.
        :param force_fetch: If True, fetch the file regardless of whether it
            has been modified since the last fetch.
        """

        url = self.__GET_FILE_TEMPLATE % {'feed_name': feed_name,
                                          'filename': filename}
        req_headers = dict(self.__default_req_headers)

        last_fetched = self.__last_fetched.get((feed_name, filename))
        if last_fetched and not force_fetch:
            req_headers['If-Modified-Since'] = email.utils.formatdate(
                    last_fetched, usegmt=True)

        try:
            resp_status, resp_headers, resp_body = utils.http_request(
                    self.feeds_host, url, req_headers=req_headers,
                    tmp_dir=self.tmp_file_dir, timeout=self.http_timeout)
        except utils.InvalidMD5Error as e:
            raise InvalidMD5Error(e)

        if resp_status == 304:
            # No changes since last_fetched.
            return None
        elif resp_status == 404:
            # File not found.
            raise NoSuchFileInFeedError(feed_name, filename)
        elif not 200 <= resp_status <= 299:
            # Other errors.
            raise FeedsWebAPIServerError(resp_body.read())

        last_modified = int(email.utils.mktime_tz(email.utils.parsedate_tz(
                resp_headers['last-modified'])))
        self.__last_fetched[(feed_name, filename)] = last_modified

        # Make sure file pointer is reset before consumer reads it.
        resp_body.seek(0)
        return resp_body

