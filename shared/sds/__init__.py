"""SDS client API.

Exposes SDSClient class to perform single and
multiple asynchronous queries to SDS service.


Example usage:

    import logging

    from shared.sds import SDSClient

    SDS_HOST = 'v1.sds.cisco.com'
    SDS_CERT = <path to client certificate>

    # Template for Web category API request
    SDS_WEB_CATEGORY_URL = '/score/webcat/json?url=%s'

    # Template for Web categories dictionary API request
    SDS_CATEGORY_DICT_URL = '/labels/webcat/json'

    # Request header required by SDS Web API for querying category information
    CATEGORIES_VERSION_HEADER = {'X-SDS-Categories-Version': 'v2'}


    clnt = SDSClient(SDS_HOST, SDS_CERT, 'test_user', 'SDSClientExample',
                     log=logging.getLogger())

    domains = ['google.com',
               'sedoparking.com',
               'yahoo.com',
               'ya.ru']

    print clnt.get_server_status()
    print clnt.query_single(SDS_CATEGORY_DICT_URL)
    print clnt.query(SDS_WEB_CATEGORY_URL, payload=domains,
                     request_headers=CATEGORIES_VERSION_HEADER)


Note: SDSClient instance is not reentrant. i.e. if the intention
is to use it in multithreaded environment - each application thread
must have it's own instance initialized.

:author: vkuznets
"""

import httplib
import json
import logging
import pycurl
import time

from cStringIO import StringIO

import shared.net.domainutils
import shared.net.registrar


SDS_STATUS_URL = '/status'

COLON                   = ':'
SEMICOLON               = ';'
CONTENT_TYPE_HDR        = 'content-type'
APPLICATION_JSON        = 'application/json'
TEXT_PLAIN              = 'text/plain'
SDS_CLIENT_USER_AGENT   = 'Cisco SDS Client'

SUPPORTED_CONTENT_TYPES = (TEXT_PLAIN, APPLICATION_JSON)

# Number of parallel Curl connections
NUM_CONNECTIONS         = 4
# Curl connection timeout, seconds
CONNECTION_TIMEOUT      = 10
# Either to use Curl in verbose mode.
VERBOSE                 = False


class SDSClientError(Exception):
    """Base SDS client error."""
    pass


class SDSServerUnreachable(SDSClientError):
    """SDS server could not be reached."""
    pass


class SDSClientSSLCertError(SDSClientError):
    """Problem with client SSL certificate."""
    pass


class SDSServerHTTPError(SDSClientError):
    """SDS server replied with not OK HTTP response."""
    pass


class SDSServerInvalidResponse(SDSClientError):
    """SDS server replied with invalid response."""
    pass


class SDSOperationTimeout(SDSClientError):
    """SDS server query hit timeout threshold."""
    pass


class SDSClient(object):
    """Generic SDS API client.

    Can be subclassed or aggregated to provide
    more specific APIs like - WBRS, WebCat, SBRS etc.
    """

    def __init__(self, sds_host, ssl_cert_file, client_id, product_id,
                 num_connections=NUM_CONNECTIONS, timeout=CONNECTION_TIMEOUT,
                 log=logging.getLogger(), cache_maxitems=0,
                 cache_ttl_multiplier=1.0, cert_ca_pem_file=None):

        """Constructor.

        :param sds_host:        SDS server host.
        :param ssl_cert_file:   Path to client SSL certificate. Must be a valid
                                X.509 certificate signed by a CA trusted by SDS.
        :param client_id:       Client ID string. Used by SDS server to identify
                                individual clients.
        :param product_id:      Product ID string. Used by SDS server to
                                identify the product requesting data.
        :param num_connections: Maximum number of simultaneous connections to
                                SDS server.
        :param timeout:         Timeout for single query.
        :param log:             Logger instance.
        :param cache_maxitems:  Maximum number of SDS cache hinting patterns to
                                store in an LRU cache. 0 to disable cache.
                                Cache only implemented for query() method calls
        :param cache_ttl_multiplier: If cache enabled apply this multiplier to
                                SDS' cache hinting TTL value. ex: use 0.5 to
                                cut in half and 2.0 to double it.
        :param cert_ca_pem_file: Full unix path to a SSL certificate authority
                                PEM file if wish to validate the SDS server
                                cert. If None it will not be verified. See note
                                below.

        CA PEM file: If you'd like to verify but don't have a PEM file search
        google or you can use //prod/main/_is/shared/python/sds/cacert.pem.
        """

        self.client_id = client_id
        self.product_id = product_id
        self.sds_host = sds_host
        self.ssl_cert_file = ssl_cert_file
        self.num_connections = num_connections
        self.timeout = timeout
        self.log = log
        self.cache_maxitems = cache_maxitems
        self.cache_ttl_multiplier = cache_ttl_multiplier
        self.cert_ca_pem_file = cert_ca_pem_file

        self.single_query_handle = None
        self.multi_query = None

        self._cache = None
        if cache_maxitems:
            shared.net.registrar.init_registrar() # See _split_url() method.
            self._cache = SDSResponseCache(cache_maxitems, log)

    def get_server_status(self):
        """The method is used to ensure the SDS host is up and running properly.

        :return: 'OK' string.

        :raises: SDSServerUnreachable, SDSClientSSLCertError,
                 SDSServerHTTPError
        """

        return self.query_single(SDS_STATUS_URL)


    def query(self, api_url, payload=None, request_headers=None,
        bypass_cache=False):
        """Perform SDS server query.

        Depending on payload this can be either single or
        multiple asynchronous query.

        :param api_url:         SDS service API URL. e.g. '/status' to request
                                for service status. If payload is not None,
                                `api_url` should be in form of Python format
                                string that will produce SDS request URLs as
                                result of `api_url % payload_item` interpolation.

                                Example:

                                    `api_url`: /score/wbrs/json?url=%s&ip=%s
                                    `payload`: [('google.com', '173.194.39.103'),
                                                ('yahoo.com', '98.139.183.24')]

                                    Will result in:

                                        /score/wbrs/json?url=google.com&ip=173.194.39.103
                                        /score/wbrs/json?url=yahoo.com&ip=98.139.183.24

        :param payload:         Optional payload. Allowed formats:

                                  - single URL or IP string;
                                  - single (URL, IP) tuple;
                                  - the list of URL or IP strings;
                                  - the list of (URL, IP) tuples;
        :param request_headers: Custom headers client may want to specify. e.g.
                                X-SDS-Categories-Version header for WebCat
                                service requests.
        :param bypass_cache:    Boolean, Ignore the cache (if enabled) for this
                                one query. Note: Cache hits get the following
                                key added to the SDS response:
                                    {<url>: [{'meta': {'_cachehit': True}}]}

        :return:                If `payload` is None: either single decoded JSON
                                object or plain-text string, depending on content
                                type reported by SDS server.

                                If `payload` is not None: dictionary of decoded
                                JSON objects per payload item. e.g.

                                 {'google.com': [{u'elements': [u'google.com'],
                                                  u'meta': {u'cache': u'google.com',
                                                  u'ttl': 3600},
                                                  u'response': {u'webcat': {u'cat': 1020}}}]}

                                If any error occurred for particular payload
                                item then dictionary value will contain an
                                instance of SDSClientError exception. e.g.

                                 {'google.com': [{u'exception': SDSServerHTTPError('...')}]}

        :raises:                SDSServerUnreachable, SDSClientSSLCertError,
                                SDSServerHTTPError, TypeError
        """

        if payload is None:
            return self.query_single(api_url, payload,
                                     request_headers=request_headers)

        if isinstance(payload, (basestring, tuple)):
            payload = [payload]

        if isinstance(payload, list):
            results = {} # {url: [{sds response}]}
            urldom = {} # {url: domain}
            cached = {}

            # Check the cache. Parse URLs to get domain.
            if self._cache and not bypass_cache:
                for idx, url in enumerate(payload):
                    urldom[url], subdom, path = self._split_url(url)
                    response = self._cache.get(urldom[url], subdom, path)
                    if response:
                        cached[url] = response
                        cached[url][0]['meta']['_cachehit'] = True
                        payload.pop(idx)
                if not payload:
                    return cached

            # First do the single query to handle and raise
            # possible SDS server errors.
            results[payload[0]] = self.query_single(api_url, payload[0],
                                          request_headers=request_headers)

            if len(payload) > 1:
                # Perform multiple asynchronous query for the
                # rest of payload items.
                if self.multi_query is None:
                    self.multi_query = SDSMultiQuery(self.sds_host,
                                                     self.ssl_cert_file,
                                                     self.client_id,
                                                     self.product_id,
                                                     self.num_connections,
                                                     self.timeout,
                                                     self.log,
                                                     self.cert_ca_pem_file)

                results.update(self.multi_query.run(api_url, payload[1:],
                                                 request_headers=request_headers))

            # Update cache.
            if self._cache and not bypass_cache:
                for url, response in results.iteritems():
                    response[0]['meta']['_cachehit'] = False
                    pattern = response[0]['meta']['cache']
                    ttl = response[0]['meta']['ttl'] * self.cache_ttl_multiplier
                    self._cache.set(urldom[url], response, pattern, ttl)
                results.update(cached)

            return results

        raise TypeError('Invalid parameter type: %s' % (type(payload),))

    def _split_url(self, url):
        # Note: shared.net.registrar.init_registrar() must be run.
        dom, subdom, path, proto, port = shared.net.domainutils.split_url(
                url=url,
                return_extra_parts=False,
                unquote_url=True,
                hash_path=False,
                lower_path=True,
                remove_tailing_slash=True,
                reverse=False,
                strict_port=True,
                add_missing_schema=True,
                encode_non_ascii=True)
        return (dom, subdom, path)

    def query_single(self, api_url, payload=None, request_headers=None,
                     response_headers=None):
        """Perform singe query to SDS server.

        :param api_url:         SDS service API URL. e.g. '/status' to request
                                for service status. If payload is not None,
                                `api_url` should be in form of Python format
                                string that will produce SDS request URL as
                                result of `api_url % payload` interpolation.

                                Example:

                                    `api_url`: /score/wbrs/json?url=%s&ip=%s
                                    `payload`: ('google.com', '173.194.39.103')

                                    Will result in:

                                        /score/wbrs/json?url=google.com&ip=173.194.39.103

        :param payload:          Optional payload. e.g. URL or IP string
                                 or (URL, IP) tuple.
        :param request_headers:  Custom request headers client may want to
                                 specify. e.g. X-SDS-Categories-Version header
                                 for WebCat service requests.
        :param response_headers: Dictionary. If provided - response headers
                                 from SDS server will be written there.

        :return:                 Decoded JSON object or text string.
                                   e.g. [{u'elements': [u'google.com'],
                                          u'meta': {u'cache': u'google.com',
                                                    u'ttl': 3600},
                                          u'response': {u'webcat': {u'cat': 1020}}}]

        :raises:                  SDSServerUnreachable, SDSClientSSLCertError,
                                  SDSServerHTTPError, TypeError
        """

        if self.single_query_handle is None:
            self.single_query_handle = _make_handle(self.ssl_cert_file,
                                                    self.timeout,
                                                    self.cert_ca_pem_file)

        try:
            self.single_query_handle.body = StringIO()
            self.single_query_handle.headers = StringIO()
            query_url = _make_sds_url(self.sds_host, api_url, payload)
            reqhdrs = _set_request_headers(self.single_query_handle,
                                 self.client_id, self.product_id,
                                 request_headers)
            self.log.debug('SDS request:\nrequest_headers:%s\nquery_url:%s',
                reqhdrs, query_url)

            self.single_query_handle.setopt(pycurl.URL, query_url)
            self.single_query_handle.setopt(pycurl.WRITEFUNCTION,
                                            self.single_query_handle.body.write)
            self.single_query_handle.setopt(pycurl.HEADERFUNCTION,
                                            self.single_query_handle.headers.write)

            self.single_query_handle.perform()

            return _handle_sds_response(self.single_query_handle,
                                        response_headers=response_headers,
                                        logger=self.log)
        except pycurl.error as err:
            self.log.error('Curl error: %s', err)
            if err.args[0] in (pycurl.E_COULDNT_RESOLVE_HOST,
                               pycurl.E_COULDNT_CONNECT):
                raise SDSServerUnreachable(err[1])
            if err.args[0] == pycurl.E_SSL_CERTPROBLEM:
                raise SDSClientSSLCertError(err[1])
            if err.args[0] == pycurl.E_OPERATION_TIMEOUTED:
                raise SDSOperationTimeout(err[1])
            raise SDSClientError(err)


    def close(self):
        """Closes all opened connections."""

        if self.single_query_handle is not None:
            self.single_query_handle.close()
            self.single_query_handle = None

        if self.multi_query is not None:
            self.multi_query.close()
            self.multi_query = None


    def __enter__(self):
        return self


    def __exit__(self, type_, value, traceback):
        self.close()



class SDSMultiQuery(object):
    """Perform multiple asynchronous queries to SDS server."""

    def __init__(self, sds_host, ssl_cert_file, client_id, product_id,
                 num_connections, timeout, log=logging.getLogger(),
                 cert_ca_pem_file=None):

        self.curl = pycurl.CurlMulti()
        self.handles = [_make_handle(
                            ssl_cert_file,
                            timeout,
                            cert_ca_pem_file) for _ in xrange(num_connections)]
        self.freelist = self.handles[:]
        self.sds_host = sds_host
        self.client_id = client_id
        self.product_id = product_id
        self.log = log
        self.cert_ca_pem_file = cert_ca_pem_file


    def run(self, api_url, payload, request_headers=None):
        """Perform multiple asynchronous queries to SDS server.

        :param api_url:          SDS service API URL. e.g. '/status'
                                 to request for service status.
        :param payload:          Payload - the list of URL/IP strings or
                                 (URL, IP) tuples.
        :param request_headers:  Custom request headers client may want to
                                 specify. e.g. X-SDS-Categories-Version header
                                 for WebCat service requests.

        :return:                 Dictionary of decoded JSON objects. e.g.

                                  {'google.com': [{u'elements': [u'google.com'],
                                                   u'meta': {u'cache': u'google.com',
                                                   u'ttl': 3600},
                                                   u'response': {u'webcat': {u'cat': 1020}}}]}

                                 If any error occurred for particular payload item
                                 then dictionary value will contain an instance of
                                 SDSClientError exception. e.g.

                                  {'google.com': [{u'exception': SDSServerHTTPError('...')}]}
        """

        if not self.handles:
            raise SDSClientError('Operation on closed SDSMultiQuery object.')

        if not isinstance(payload, (list, tuple)):
            raise TypeError('Invalid parameter type: %s' % (type(payload),))

        payload_len = len(payload)
        num_submitted = 0
        num_processed = 0
        result = {}

        while num_processed < payload_len:
            while self.freelist and (num_submitted < payload_len):
                handle = self.freelist.pop()
                query_item = payload[num_submitted]
                self.__submit_handle(handle, api_url, query_item,
                                     request_headers=request_headers)
                num_submitted += 1
            self.__perform()
            num_processed += self.__collect_result(result)

            self.curl.select(0.5)

        return result


    def close(self):
        """Close all open handles. SDSMultiQuery object becomes
        unusable after this call.
        """

        for handle in self.handles:
            handle.close()

        self.curl.close()

        self.handles = None
        self.freelist = None
        self.curl = None


    # Private stuff

    def __submit_handle(self, handle, api_url, query_item,
                        request_headers=None):
        """Allocate new handle and submit it for processing."""

        handle.body = StringIO()
        handle.headers = StringIO()
        sds_url = _make_sds_url(self.sds_host, api_url, query_item)
        _set_request_headers(handle, self.client_id, self.product_id,
                             request_headers)
        handle.setopt(pycurl.URL, sds_url)
        handle.setopt(pycurl.WRITEFUNCTION, handle.body.write)
        handle.setopt(pycurl.HEADERFUNCTION, handle.headers.write)

        handle.query_item = query_item
        self.curl.add_handle(handle)


    def __free_handle(self, handle):
        """Return handle to the freelist."""

        handle.body = None
        self.curl.remove_handle(handle)
        self.freelist.append(handle)


    def __perform(self):
        """Run the internal curl state machine for the multi stack."""

        ret = pycurl.E_CALL_MULTI_PERFORM
        while ret == pycurl.E_CALL_MULTI_PERFORM:
            ret, _ = self.curl.perform()


    def __collect_result(self, result):
        """Check for curl objects which have terminated, collect
           result and add them to the freelist.

        :param result: (out) dictionary to collect query results.

        :return:       the number of finished queries.
        """

        queries_finished = 0

        while True:
            num_q, ok_list, err_list = self.curl.info_read()
            for handle in ok_list:
                result[handle.query_item] = _handle_sds_response(handle,
                        raise_exc=False)
                self.__free_handle(handle)

            for handle, errno, errmsg in err_list:
                self.log.warning('Query for %s failed: %s, %s',
                                 handle.query_item, errno, errmsg)

                exception = SDSClientError('%s %s' % (errno, errmsg,))
                if errno in (pycurl.E_COULDNT_RESOLVE_HOST,
                             pycurl.E_COULDNT_CONNECT):
                    exception = SDSServerUnreachable(errmsg)
                if errno == pycurl.E_SSL_CERTPROBLEM:
                    exception = SDSClientSSLCertError(errmsg)

                result[handle.query_item] = [{u'exception': exception}]

                self.__free_handle(handle)

            queries_finished += (len(ok_list) + len(err_list))

            if num_q == 0:
                break

        return queries_finished


class SDSResponseCache(object):

    """SDS response cache.

    LRU cache.
    Per-entry TTL.
    Supports SDS' wildcard cache-hinting format.
    Store's arbitrary application-specific objects. (ex: WBRS Score)
    """

    def __init__(self, maxsize, logger=None):
        """Constructor.

        :param maxsize: int, maximum entries.
        :param logger: logging object, initialized logger.
        """
        # Patterns stored in a dict with domain as key. Linked list used for
        # LRU order. Nodes are the dict entries. Node format:
        #   <str:domain>:
        #       left: <str:dict key> # Link list node
        #       right: <str:dict key> # Link list node
        #       patterns:
        #           <str:sds pattern>:
        #               value: <object> # ex: WBRS signed float score.
        #               expire: <int:unix epoch when TTL is up>

        self._maxsize = maxsize
        self._log = logger or logging.getLogger(self.__class__.__name__)

        self._started = time.time()
        self.init()

    def init(self):
        """WARNING: Remove all nodes from cache and start over!"""
        self._cache = {}
        self._newest = None  # Right most linked list node.
        self._oldest = None  # Left most linked list node.
        self._size = 0
        self._hits = 0
        self._misses = 0

    def set(self, domain, value, pattern, ttl):
        """Add a new SDS pattern to the cache.

        :param domain: str
        :param value: object. Currenty used for WBRS score. Could be replaced
            to hold category info, etc..
        :param pattern: str, SDS cache hinting wildcard pattern
        :param ttl: int, cache entry time to live in seconds
        """
        dom = domain.lower()
        if dom not in self._cache:
            self._cache[dom] = {
                'left': self._newest,
                'right': None, # We're newest one.
                'patterns': {},
                }
            if self._size:
                self._cache[self._newest]['right'] = dom
            else:
                self._oldest = dom
            self._newest = dom
        node = self._cache[dom]
        if pattern not in node['patterns']:
            self._size += 1
        node['patterns'][pattern] = {
            'value': value,
            'expire': time.time() + ttl,
            }
        while self._size > self._maxsize:
            self.remove(self._oldest)
        self._log.debug('set(): %s -> new entry: %s', self, node)

    def get(self, domain, subdomain, path):
        """Return cache value if URL parts match SDS pattern.

        See wildcard_url_pattern_match() docstring for domain, subdomain, and
        path params.
        :return: object (cached value) or None
        """
        dom = domain.lower()
        data = self._cache.get(dom)

        if not data:
            self._misses += 1
            return None

        now = time.time()
        # First purge expired patterns.
        for pat in data['patterns'].keys():
            if data['patterns'][pat]['expire'] <= now:
                del data['patterns'][pat]
                self._size -= 1
        if not data['patterns']:
            self.remove(dom)
            self._misses += 1
            return None

        self._promote(dom) # Most recently used.

        # Do the lookup.
        matchpat = SDSResponseCache.wildcard_url_pattern_match(
            data['patterns'], domain, subdomain, path)
        if matchpat:
            self._hits += 1
            return data['patterns'][matchpat]['value']
        else:
            self._misses += 1
            return None

    def remove(self, domain, delete_node=True):
        """Remove the node from the cache and all its patterns.

        :param domain: str,
        :param delete_node: boolean, delete the underlying node structure?
            Set to False if you wish to move it or store it elsewhere.
        :return: dict (the node) if delete_node=False, else None
        """
        dom = domain.lower()
        data = self._cache.get(dom)
        if data:
            dom_left, dom_right = (data['left'], data['right'])
            node_left = self._cache.get(dom_left)
            node_right = self._cache.get(dom_right)

            if node_left is None and node_right is None:
                # Last node in cache.
                self._oldest = None
                self._newest = None
                self._size = 0
            elif node_left is None:
                # Deleting the oldest node.
                node_right['left'] = None
                self._oldest = dom_right
            elif node_right is None:
                # Deleting the newest node.
                node_left['right'] = None
                self._newest = dom_left
            else:
                # Deleting node in the middle.
                node_left['right'] = data['right']
                node_right['left'] = data['left']

            if self._size:
                self._size -= len(data['patterns'])
            if not delete_node:
                data['_removed'] = time.time()
                return data
            else:
                del self._cache[dom]
                return None

    @staticmethod
    def wildcard_url_pattern_match(patterns, domain, subdomain, path, log=None):
        """Return the most significant pattern matched for given URL parts.

        :param patterns: list of str, URLs with leading and/or training splat wildcard
            character or exact match. URL can only contain (sub)domain and path. Ex:
            *.foo.bar.com/some/path/*, *.bar.com/*, sub1.bar.com, bar.com/a/b/c.jpg/*.
            Notice tailing splat always has a slash. This is the format SDS uses
            for their cache hinting.
        :param domain: str, ex: 'foo.com' from 'a.b.c.foo.com'.
        :param subdomain: str, just subdomain part of URL else null string if none.
            ex: 'a.b.c' from 'a.b.c.foo.com'.
        :param path: str, URL path to lookup or null string if none. No CGI params.
        :param log: object, optional initalized logging-module logger.
        :return: str, most significant pattern that matched else None

        Significance is determine by the count of path elements plus count of
        subdomains. Exact matches (non wildcard patterns) are most significant.
        Matches are case insensitive. This is a staticmethod to aid testing.
        """
        mt = 'wildcard_url_pattern_match()'

        if not patterns or not domain:
            return None

        lpats = [p.lower() for p in patterns]

        url = subdomain + '.' if subdomain else ''
        url += domain
        url += '/' + path if path and not path.startswith('/') else path
        url = url.lower()
        if log:
            log.debug('%s: Matching against this URL: %s', mt, url)

        if url in lpats:
            return url # Exact match.

        hits = [] # (significance, pattern)
        for pat in lpats:
            wildhead, wildtail = (pat.startswith('*.'), pat.endswith('/*'))
            result = 'miss'
            if (wildhead and wildtail and pat[2:-2] in url) \
                or (wildhead and not wildtail and url.endswith(pat[2:])) \
                or (not wildhead and wildtail and url.startswith(pat[:-2])):
                signif = pat.count('/') + pat.split('/')[0].count('.')
                hits.append((signif, pat))
                result = 'hit (significance: %s)' % signif
            if log:
                log.debug('%s: pattern[%s] -> %s', mt, pat, result)
        if hits:
            return sorted(hits)[-1][-1]

        return None

    def domain_list(self):
        """Return list of all nodes and their unexpired pattern counts.

        :return: [(<str:domain_name>, <int:unexpired_pattern_count>), ...]
            in order most recently used to least.
        """
        dom = self._newest
        if not dom:
            return []

        now = time.time()
        doms = [] # (domain, non-expired pattern count), newest first
        while True:
            count = 0
            for k, data in self._cache[dom]['patterns'].iteritems():
                if data['expire'] > now:
                    count += 1
            doms.append((dom, count))
            dom = self._cache[dom]['left']
            if not dom:
                break

        return doms

    def _promote(self, domain):
        """Moves node matching domain to top of linked list."""
        dom = domain.lower()
        data = self._cache.get(dom)
        if data and data['right'] != None:
            self.remove(dom, delete_node=False)
            self._size += len(data['patterns']) # The remove decremented.
            self._cache[self._newest]['right'] = dom
            data['left'] = self._newest
            data['right'] = None
            self._newest = dom

    def __str__(self):
        return '<%s size=%s maxsize=%s oldest=%s newest=%s hits=%s misses=%s>'\
            % (self.__class__.__name__, self._size, self._maxsize,
            self._oldest, self._newest, self._hits, self._misses)


def _handle_sds_response(handle, raise_exc=True, response_headers=None,
        logger=None):
    """Extract and process SDS server response from Curl handle.

    :param handle:      Curl handle.
    :param raise_exc:   Either to raise exception on HTTP error.
                        If False - exception instance will not be raised
                        but returned in the result instead. It's useful
                        for multiple queries when it is undesirable to
                        raise an exception if one or several queries in
                        batch failed.
    :response_headers:  Dictionary. If provided - response headers
                        from SDS server will be written there.
    :param logger:      Logger instance.

    :return:           decoded JSON object or text string.
                       e.g. [{u'elements': [u'google.com'],
                              u'meta': {u'cache': u'google.com',
                                        u'ttl': 3600},
                              u'response': {u'webcat': {u'cat': 1020}}}]

    :raises:           SDSServerHTTPError
    """

    resp = handle.body.getvalue()
    http_code = handle.getinfo(pycurl.HTTP_CODE)
    handle.headers.seek(0)
    content_type = _parse_response_headers(handle.headers, response_headers)

    if logger:
        logger.debug('SDS response:\nhttp_code:%s\ncontent_type:%s\n'
            'response_headers:%s\nresponse_body:%s', http_code, content_type,
            response_headers, resp)

    exception = None
    if http_code != httplib.OK:
        exception = SDSServerHTTPError('HTTP code %d. Error message: %s' %
                                       (http_code, resp,))

    if content_type not in SUPPORTED_CONTENT_TYPES:
        msg = 'SSL certificate error'
        if http_code == 400 and msg in resp:
            exception = SDSClientSSLCertError(msg)
        else:
            exception = SDSServerInvalidResponse('Unsupported content type: '
                                             '%s' % (content_type,))
    if exception is not None:
        if raise_exc:
            raise exception
        return [{u'exception': exception}]

    if content_type == APPLICATION_JSON:
        return json.loads(resp)

    return resp


def _parse_response_headers(raw_headers, resp_header_dict=None):
    """Parse raw HTTP response headers, extract Content-type parameter
    and(optionally) produce the output dictionary with parsed key/value
    header pairs.

    :param raw_headers:      iterable which contains raw response headers
                             string.
    :param resp_header_dict: (out) dictionary where parsed response headers
                             will be written.

    :return:                 value of Content-type header.
    """

    content_type = None
    for line in raw_headers:
        if COLON in line:
            parts = line.split(COLON)
            hdr_name = parts[0].strip().lower()
            hdr_val = COLON.join(parts[1:]).strip().lower()
            if hdr_name and hdr_val:
                if hdr_name == CONTENT_TYPE_HDR:
                    content_type = hdr_val.split(SEMICOLON)[0].strip()
                    if resp_header_dict is None:
                        # Do early exit if we're not interested
                        # in response headers.
                        return content_type

                if resp_header_dict is not None:
                    resp_header_dict[hdr_name] = hdr_val

    return content_type


def _make_handle(ssl_cert_file, timeout, ca_pem_file=None):
    """Create Curl handle."""

    handle = pycurl.Curl()

    handle.setopt(pycurl.SSL_VERIFYPEER, 0)
    if ca_pem_file:
        handle.setopt(pycurl.SSL_VERIFYHOST, 2)
        handle.setopt(pycurl.CAINFO, ca_pem_file)
    else:
        handle.setopt(pycurl.SSL_VERIFYHOST, 0)
    handle.setopt(pycurl.SSLCERT, ssl_cert_file)
    handle.setopt(pycurl.NOSIGNAL, 1)
    handle.setopt(pycurl.CONNECTTIMEOUT, timeout)
    handle.setopt(pycurl.TIMEOUT, timeout)
    handle.setopt(pycurl.IPRESOLVE, pycurl.IPRESOLVE_V4)
    handle.setopt(pycurl.USERAGENT, SDS_CLIENT_USER_AGENT)

    # Set verbose mode for Curl if needed.
    if VERBOSE:
        def curl_debug_function(debug_type, debug_msg):
            if debug_type == pycurl.INFOTYPE_TEXT:
                logging.getLogger('curl').debug(debug_msg.strip())

        handle.setopt(pycurl.VERBOSE, 1)
        handle.setopt(pycurl.DEBUGFUNCTION, curl_debug_function)

    handle.body = None
    handle.query_item = None

    return handle


def _set_request_headers(handle, client_id, product_id, headers=None):
    """Set HTTP request headers to the Curl handle.

    This method sets some required HTTP headers such as
    'X-Client-ID' and 'X-Product-ID' along with optional
    request headers(e.g. 'X-SDS-Categories-Version')
    provided in `headers` dictionary.
    """

    # Add required headers.
    header_list = ['X-Client-ID: %s' % (client_id,),
                   'X-Product-ID: %s' % (product_id,)]

    # Add optional headers if there are any.
    if headers is not None:
        for name, val in headers.iteritems():
            header_list.append('%s: %s' % (name, val))

    handle.setopt(pycurl.HTTPHEADER, header_list)

    return header_list

def _make_sds_url(sds_host, api_url, payload=None):
    """Construct SDS query URL based on SDS host, API URL
       template and payload."""

    if payload is not None:
        api_url = api_url % payload

    return 'https://%s%s' % (sds_host, api_url)
