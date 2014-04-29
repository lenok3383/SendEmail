#!/usr/bin/python

"""
test_taxonomy_converter.py

Test suite to test AVCTaxonomyConverter class.

:Status: $Id: //prod/main/_is/shared/python/avc/test/test_taxonomy_converter.py#2 $
:Author: ivlesnik
:Last Modified By: $Author: ivlesnik $
:Date: $Date: 2013/10/17 $
"""

import os
import unittest2

import shared.avc.taxonomy_converter
from shared.avc.taxonomy_converter import (
        AVCTaxonomyConverter, AVCTaxonomyConverterError, PRE_FALCON_MODE)
from shared.testing.vmock import mockcontrol as mc
from shared.testing.vmock.helpers.filemock import FileMock

WSA_TAXONOMY = {
     14: {'actions': 'block',
          'entry_type': 'type',
          'feature_avc': '1',
          'mnemonic': 'games',
          'name': 'Games'},
     209: {'actions': 'block',
           'dialog': 'Block Unsafe Content (no rewrite)',
           'entry_type': 'behavior',
           'feature_enforce_ratings': '1',
           'feature_safe_search': '1',
           'mnemonic': 'unsafeblock',
           'name': 'Unsafe Content'},
     1008: {'actions': 'block',
            'entry_type': 'type',
            'feature_avc': '1',
            'mnemonic': 'google_im',
            'name': 'Google Talk'},
     1016: {'actions': 'block',
            'app_type': 14,
            'behaviors': '209',
            'entry_type': 'app',
            'feature_safe_search': '1',
            'mnemonic': 'bing',
            'name': 'Bing',
            'rewrite_rules': '9009'},
     1020: {'actions': 'block throttle_user',
            'app_type': 1008,
            'entry_type': 'app',
            'feature_avc': '1',
            'feature_global_bandwidth': '1',
            'mnemonic': 'silverlight',
            'name': 'Silverlight'},
     1030: {'actions': 'block',
            'app_type': 1008,
            'behaviors': '209',
            'entry_type': 'app',
            'feature_avc': '1',
            'mnemonic': 'facebookevents',
            'name': 'Facebook Events'},
     9009: {'entry_type': 'rewrite',
            'mnemonic': 'bing_rewrite_rule',
            'name': 'bing_rewrite_rule',
            'rewrite_text': 'rewrite_rule ( { url tokenize ? { 1-1 }...'}}

PRE_FALCON_TAXONOMY = {
     14: {'actions': 'block',
          'entry_type': 'type',
          'feature_avc': '1',
          'mnemonic': 'games',
          'name': 'Games'},
     200: {'actions': 'block',
           'entry_type': 'behavior',
           'mnemonic': 'generic',
           'name': 'Generic'},
     209: {'actions': 'block',
           'dialog': 'Block Unsafe Content (no rewrite)',
           'entry_type': 'behavior',
           'feature_enforce_ratings': '1',
           'feature_safe_search': '1',
           'mnemonic': 'unsafeblock',
           'name': 'Unsafe Content'},
     1008: {'actions': 'block',
            'description': 'Google Talk is an instant messenging...',
            'entry_type': 'type',
            'feature_avc': '1',
            'mnemonic': 'google_im',
            'name': 'Google Talk',
            'reference': 'http://www.google.com/talk, ...',
            'sig_dependency': 'To enable full coverage for this ...'},
     1016: {'actions': 'block',
            'app_type': 14,
            'behaviors': '200 209',
            'description': 'Bing (formerly Live Search, Windows ',
            'entry_type': 'app',
            'feature_safe_search': '1',
            'mnemonic': 'bing',
            'name': 'Bing',
            'reference': 'http://www.bing.com,http://en.wikipedia.org',
            'rewrite_rules': '9009'},
     1020: {'actions': 'block throttle_user',
            'app_type': 1008,
            'behaviors': '200',
            'entry_type': 'app',
            'feature_avc': '1',
            'feature_global_bandwidth': '1',
            'mnemonic': 'silverlight',
            'name': 'Silverlight'},
     1030: {'actions': 'block',
            'app_type': 1008,
            'behaviors': '209',
            'entry_type': 'app',
            'feature_avc': '1',
            'mnemonic': 'facebookevents',
            'name': 'Facebook Events'},
     9009: {'entry_type': 'rewrite',
            'mnemonic': 'bing_rewrite_rule',
            'name': 'bing_rewrite_rule',
            'rewrite_text': 'rewrite_rule ( { url tokenize ? { 1-1 }...'}}


class TestAVCTaxonomyConverter(unittest2.TestCase):

    def setUp(self):
        """Sets up the unit tests."""

        self.mc = mc.MockControl()

        self.converter = AVCTaxonomyConverter()
        self.taxonomy = {
            2:    {'name': 'Instant Messaging',
                   'mnemonic': 'im',
                   'actions': 'block',
                   'entry_type': 'app',
                   'parent': '0',
                   'feature_avc': '1'},
            14:   {'name': 'Games',
                   'mnemonic': 'games',
                   'actions': 'block',
                   'entry_type': 'app',
                   'parent': '0',
                   'feature_avc': '1'},
            200:  {'name': 'Generic',
                   'mnemonic': 'generic',
                   'actions': 'block',
                   'entry_type': 'behavior'},
            209:  {'name': 'Unsafe Content',
                   'dialog': 'Block Unsafe Content (no rewrite)',
                   'mnemonic': 'unsafeblock',
                   'actions': 'block',
                   'entry_type': 'behavior',
                   'feature_enforce_ratings': '1',
                   'feature_safe_search': '1'},
            1008: {'name': 'Google Talk',
                   'mnemonic': 'google_im',
                   'parent': '2',
                   'actions': 'block',
                   'feature_avc': '1',
                   'entry_type': 'app',
                   'behaviors': '200',
                   'description': 'Google Talk is an instant messenging...',
                   'reference': 'http://www.google.com/talk, ...',
                   'sig_dependency': 'To enable full coverage for this ...'},
            1016: {'actions': 'block', 'parent': '14',
                   'behaviors': '200 209', 'entry_type': 'app',
                   'feature_safe_search': '1', 'mnemonic': 'bing',
                   'description': 'Bing (formerly Live Search, Windows ',
                   'reference': 'http://www.bing.com,http://en.wikipedia.org',
                   'name': 'Bing', 'rewrite_rules': '9009'},
            1020: {'actions': 'block throttle_user', 'parent': '1008',
                   'entry_type': 'app', 'feature_avc': '1',
                   'feature_global_bandwidth': '1', 'behaviors': '200',
                   'mnemonic': 'silverlight', 'name': 'Silverlight'},
            1030: {'actions': 'block', 'parent': '1008',
                   'behaviors': '209', 'entry_type': 'app',
                   'feature_avc': '1', 'mnemonic': 'facebookevents',
                   'name': 'Facebook Events'},
            9009: {'name': 'bing_rewrite_rule',
                   'mnemonic': 'bing_rewrite_rule',
                   'rewrite_text': 'rewrite_rule ( { url tokenize ? { 1-1 }...',
                   'entry_type': 'rewrite'},
        }

    def tearDown(self):
        """Tides up after unit tests."""
        self.mc.tear_down()

    def test_is_leaf_node(self):
        """Test _is_leaf_node method."""
        self.assertTrue(self.converter._is_leaf_node(self.taxonomy, 9009))
        self.assertFalse(self.converter._is_leaf_node(self.taxonomy, 1008))
        self.assertFalse(self.converter._is_leaf_node(self.taxonomy, 2))
        self.assertTrue(self.converter._is_leaf_node(self.taxonomy, 209))
        self.assertTrue(self.converter._is_leaf_node(self.taxonomy, -1))
        self.assertTrue(self.converter._is_leaf_node(self.taxonomy, '1'))
        self.assertRaises(ValueError,
                          self.converter._is_leaf_node, self.taxonomy, 'A')

    def test_convert_fake_version(self):
        """Test convert method with fake version."""
        self.assertRaises(AVCTaxonomyConverterError, self.converter.convert,
                          self.taxonomy, to_version='FAKE')

    def test_convert_wsa(self):
        """Test convert method (default, WSA mode)."""
        taxonomy = self.converter.convert(self.taxonomy)
        self.assertEqual(taxonomy, WSA_TAXONOMY)

    def test_convert_prefalcon(self):
        """Test convert method (PRE_FALCON mode)."""
        taxonomy = self.converter.convert(self.taxonomy, mode=PRE_FALCON_MODE)
        self.assertEqual(taxonomy, PRE_FALCON_TAXONOMY)

    def test_convert_no_mnemonic(self):
        """Test convert method without one mnemonic in data."""
        del self.taxonomy[14]['mnemonic']
        self.assertRaises(AVCTaxonomyConverterError, self.converter.convert,
                          self.taxonomy)

    def test_convert_fake_exceptions(self):
        """Test convert method with fake exceptions."""
        taxonomy = self.converter.convert(self.taxonomy,
                                          exceptions={'fake': 'im'})
        self.assertEqual(taxonomy, WSA_TAXONOMY)

    def test_process_exceptions(self):
        """Test _process_exceptions method."""
        exceptions = {'bing': 'im', 'silverlight': 'FAKE'}
        mnemonics = {}
        for section in self.taxonomy:
            if self.taxonomy[section]['entry_type'] == 'app':
                mnemonics[self.taxonomy[section]['mnemonic']] = section
        taxonomy = self.converter._process_exceptions(self.taxonomy,
                                                      exceptions, mnemonics)
        self.assertEquals(taxonomy[1016]['app_type'], 2)
        self.assertEquals(taxonomy[1020]['parent'], '1008')


if __name__ == '__main__':
    unittest2.main()
