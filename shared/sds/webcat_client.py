"""WebCat/WBRS client for SDS service.


Example of WebLookupClient usage:

    from shared.sds import webcat_client

    SDS_HOST = 'v1.sds.cisco.com'
    SDS_CERT = <path to client certificate>

    clnt = webcat_client.WebLookupClient(SDS_HOST, SDS_CERT,
                                         'test_user', 'WebcatClientExample',
                                         1, log=logging.getLogger())

    domains = ['google.com',
               'sedoparking.com',
               'yahoo.com',
               'ya.ru']

    print clnt.get_status()
    print clnt.get_categories_version()
    print clnt.get_category_dict()
    print clnt.get_category(domains)
    print clnt.get_simple_wbrs_score(domains)


Note:

- WebLookupClient instance is not reentrant. i.e. if the intention is to use it
  in multithreaded environment - each application thread must have it's own
  WebLookupClient instance initialized.
- SDS Web API's still missing some features required by SecApps products. Hence
  the WebLookupClient API is not yet complete to be the full Web Lookup Client
  replacement(//prod/main/_is/products/web_lookup_server/src/python/web_lookup_client/lookup_client.py).

:author: vkuznets
"""

import logging

from shared.sds import SDSClient, SDSClientError
from shared.sds import util


# Template for simple WBRS score API request(no IP provided)
SDS_SIMPLE_WBRS_SCORE_URL = '/score/wbrs/json?url=%s'
# Template for Web category API request
SDS_WEB_CATEGORY_URL = '/score/webcat/json?url=%s'
# Template for Web categories dictionary API request
SDS_CATEGORY_DICT_URL = '/labels/webcat/json'
# X-SDS-Categories-Version HTTP header
SDS_CAT_VERSION_HEADER = 'X-SDS-Categories-Version'

# Curl connection timeout
CONNECTION_TIMEOUT = 10

# Number of parallel Curl connections.
NUM_CONNECTIONS = 4


class UnsupportedCategoriesVersion(SDSClientError):
    """Exception of this type is raised when client
    requests unsupported categories version from SDS server.
    """
    pass


class WebLookupClient(SDSClient):
    """WebCat/WBRS API client for SDS."""

    def __init__(self, sds_host, ssl_cert_file, client_id, product_id,
                 categories_version=None, log=logging.getLogger()):
        """Constructor.

        :param sds_host:           SDS server host.
        :param ssl_cert_file:      Path to client SSL certificate. Must be a
                                   valid X.509 certificate signed by a CA
                                   trusted by SDS.
        :param client_id:          Client ID string. Used by SDS server to
                                   identify individual clients.
        :param product_id:         Product ID string. Used by SDS server to
                                   identify the product requesting data.
        :param categories_version: Categories version required by client.
                                   If None - use categories version provided
                                   by SDS server.
        :param log:                Logger instance.
        """

        self.log = log
        super(WebLookupClient, self).__init__(sds_host, ssl_cert_file,
                                              client_id, product_id,
                                              NUM_CONNECTIONS,
                                              CONNECTION_TIMEOUT, log)
        self.get_status = self.get_server_status
        self.required_categories_version = categories_version
        self.server_categories_version = None

        # Refresh categories version supported by SDS server.
        self.get_category_dict()

        self.log.info('WebLookupClient instance initialized.')


    def get_categories_version(self):
        """Returns version of categories supported by SDS server."""

        self.log.debug('Executing client method: get_categories_version')

        return self.server_categories_version


    def get_category_dict(self):
        """Gets information about supported categories.

        :return: dictionary in format(note that 'mnemonic' is currently
                 missing in SDS response):
                   {category_id(str): {'name': str, 'description': str},}

        :raises: SDSServerUnreachable, SDSClientSSLCertError,
                 SDSServerHTTPError
        """

        self.log.debug('Executing client method: get_category_dict')

        resp_headers = {}
        categories = self.query_single(SDS_CATEGORY_DICT_URL,
                                       response_headers=resp_headers)
        self.server_categories_version = int(
                resp_headers[SDS_CAT_VERSION_HEADER.lower()][1:])

        # Check that server categories version match categories
        # version requested by client.
        self._check_categories_version()

        return categories


    @util.extract_web_categories
    def get_category(self, urls):
        """Returns category verdict for each URL.

        :param urls: single URL or list of URLs to get category for.

        :return: dictionary in format {url: category, ...}.
                 Category is None if URL could not be categorized.

        :raises: SDSServerUnreachable, SDSClientSSLCertError,
                 SDSServerHTTPError, TypeError
        """

        self.log.debug('Executing client method: get_category')

        headers = {SDS_CAT_VERSION_HEADER: 'v%s' % (
                self.server_categories_version,)}
        return self.query(SDS_WEB_CATEGORY_URL, payload=urls,
                          request_headers=headers)


    @util.extract_wbrs_scores
    def get_simple_wbrs_score(self, urls):
        """Returns WBRS score for each URL as a float

        :param urls: single URL or list of URLs to get score for

        :return: dictionary in format {url: score, ...}.
                 Score is None if URL could not be scored.

        :raises: SDSServerUnreachable, SDSClientSSLCertError,
                 SDSServerHTTPError, TypeError
        """

        self.log.debug('Executing client method: get_simple_wbrs_score')

        return self.query(SDS_SIMPLE_WBRS_SCORE_URL, urls)


    def _check_categories_version(self):
        """Check that server categories version match categories
        version requested by client.
        """

        if (self.required_categories_version is not None and
                self.required_categories_version != self.server_categories_version):
            raise UnsupportedCategoriesVersion(
                    'Client version: %s, server version: %s' %
                    (self.required_categories_version,
                     self.server_categories_version))
