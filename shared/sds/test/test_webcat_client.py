"""Unit tests for shared.sds.webcat_client.py module.

:Status: $Id: //prod/main/_is/shared/python/sds/test/test_webcat_client.py#1 $
:Author: aianushe
:Last Modified By: $Author: vkuznets $
"""

import copy
import inspect
import json
import logging
import os
import pycurl
import re
import unittest2 as unittest

from shared.sds import webcat_client

from pycurl_mockup import CurlMultiMockup
from pycurl_mockup import CurlMockup

CAT_VERSION = 1
SDS_HOST = 'v1.sds.cisco.com'


class TestWebLookupClient(unittest.TestCase):

    def setUp(self):
        """Setup.

        Set WebLookupClient object, mock pycurl.Curl and pycurl.CurlMulti,
        set test_data_dir.
        """
        self.old_Curl = pycurl.Curl
        pycurl.Curl = CurlMockup

        self.old_CurlMulti = pycurl.CurlMulti
        pycurl.CurlMulti = CurlMultiMockup

        self_file = inspect.getsourcefile(self.__class__)
        self_dir = os.path.dirname(os.path.abspath(self_file))
        self.test_data_dir = os.path.join(self_dir, 'data')

        category_dict_mockup_path = os.path.join(self.test_data_dir,
                                                 'category_dict_mockup.json')
        with open(category_dict_mockup_path, 'r') as data_file:
            category_dict_mockup = json.load(data_file)

        cat_dict_url = 'https://%s%s' % (SDS_HOST,
                                         webcat_client.SDS_CATEGORY_DICT_URL)
        self.category_curl_data = {cat_dict_url : category_dict_mockup}
        # Set categories data in CurlMockup required for initialization.
        CurlMockup.set_data(self.category_curl_data)

        self._client = webcat_client.WebLookupClient(SDS_HOST, None,
             'aianushe', 'DevLookupTool', CAT_VERSION, log=logging.getLogger())

    def tearDown(self):
        """Restore pycurl.Curl and pycurl.CurlMulti."""
        pycurl.Curl = self.old_Curl
        pycurl.CurlMulti = self.old_CurlMulti

    def test_get_category_dict(self):
        """Test get_category_dict() method."""
        # Set categories data in CurlMockup
        CurlMockup.set_data(self.category_curl_data)

        category_dict_expected_path = os.path.join(self.test_data_dir,
                                                 'category_dict_expected.json')
        with open(category_dict_expected_path, 'r') as data_file:
            category_dict_expected = json.load(data_file)

        self.assertEquals(self._client.get_category_dict(),
                          category_dict_expected)

        headers = self.category_curl_data[
                            self.category_curl_data.keys()[0]]['headers']
        reg_exp = ur"X-SDS-Categories-Version: v\d+"
        replace = u"X-SDS-Categories-Version: v99999999"
        wrong_version_headers = re.sub(reg_exp, replace, headers)
        wrong_version_cat_curl_data = copy.deepcopy(self.category_curl_data)
        wrong_version_cat_curl_data[
          self.category_curl_data.keys()[0]]['headers'] = wrong_version_headers

        CurlMockup.set_data(wrong_version_cat_curl_data)

        with self.assertRaises(webcat_client.UnsupportedCategoriesVersion):
            self._client.get_category_dict()

    def test_get_categories_version(self):
        """Test get_categories_version() method."""
        self.assertEquals(self._client.get_categories_version(), CAT_VERSION)

    def test_get_category(self):
        """Test get_category() method."""

        def _get_categorization_uri(item):
            """Get categorization URI."""
            return 'https://%s%s' % (
                       SDS_HOST, webcat_client.SDS_WEB_CATEGORY_URL % (item,))

        curl_mock_dict = {}
        category_mockup_path = os.path.join(self.test_data_dir,
                                            'category_mockup.json')
        with open(category_mockup_path, 'r') as data_file:
            category_mockup = json.load(data_file)

        for key, value in category_mockup.iteritems():
            curl_mock_dict[_get_categorization_uri(key)] = value
        CurlMockup.set_data(curl_mock_dict)

        result = self._client.get_category(category_mockup.keys())
        expected = {u'apple.com': 1003,
                    u'facebook.com': 1069,
                    u'http://www.youtube.com/': 1026,
                    u'www.google.com': 1020,
                    u'http://stackoverflow.com': 1003}
        self.assertEquals(result, expected)

    def test_get_simple_wbrs_score(self):
        """Test get_simple_wbrs_score() method."""

        def _get_simple_wbrs_score_uri(item):
            """Get Simple WBRS score URI."""
            return 'https://%s%s' % (
                  SDS_HOST, webcat_client.SDS_SIMPLE_WBRS_SCORE_URL % (item,))

        wbrs_score_mockup_path = os.path.join(self.test_data_dir,
                                              'wbrs_score_mockup.json')
        with open(wbrs_score_mockup_path, 'r') as data_file:
            wbrs_score_mockup = json.load(data_file)

        wbrs_score_expected_path = os.path.join(self.test_data_dir,
                                                'wbrs_score_expected.json')
        with open(wbrs_score_expected_path, 'r') as data_file:
            wbrs_score_expected = json.load(data_file)

        domains = wbrs_score_mockup.keys()

        wbrs_score_mock_dict = {}
        for key, value in wbrs_score_mockup.iteritems():
            wbrs_score_mock_dict[_get_simple_wbrs_score_uri(key)] = value
        CurlMockup.set_data(wbrs_score_mock_dict)

        result = self._client.get_simple_wbrs_score(domains)

        self.assertEquals(result, wbrs_score_expected)

if __name__ == '__main__':
    unittest.main()

