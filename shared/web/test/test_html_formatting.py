"""Test of html_formatting methods.

:Status: $Id: //prod/main/_is/shared/python/web/test/test_html_formatting.py#5 $
:Authors: scottrwi
"""

import unittest2 as unittest
from shared.web.html_formatting import list_of_lists_to_table, dict_to_table


class TestListOfLists(unittest.TestCase):

    DATA = [['field1', 'field2'], ['X<X', 'Y&Y'], ['foo', '']]

    def test_default(self):
        res = list_of_lists_to_table(self.DATA)

        # Ensure table doesn't have class
        self.assertNotEqual(res.find('<table>'), -1)

        # Ensure it escaped characters.
        self.assertTrue(res.find('X&lt;X'))
        self.assertTrue(res.find('X&amp;X'))

        # Check it highlighted first row.
        self.assertTrue(res.find('<th>field1</th>'))

        # Ensure it filled in border value.
        self.assertTrue(res.find('border="1"'))

        # Ensure it replaced an empty string.
        self.assertTrue(res.find('<td>&nbsp;</td>'))

    def test_table_class(self):
        res = list_of_lists_to_table(self.DATA, table_class='foo')

        self.assertNotEqual(res.find('<table class="foo">'), -1)

    def test_no_escape(self):
        res = list_of_lists_to_table(self.DATA, escape=False)

        self.assertTrue(res.find('X<X'))
        self.assertTrue(res.find('X&X'))

    def test_no_highlight(self):
        res = list_of_lists_to_table(self.DATA, highlight_first_row=False)

        self.assertTrue(res.find('<td>field1</td>'))


class TestDict(unittest.TestCase):
    DATA = {'key1': 'val1'}

    def test_dict(self):
        res = dict_to_table(self.DATA)

        self.assertTrue(res.find('<th>Key</th>'))
        self.assertTrue(res.find('<th>Value</th>'))


if __name__ == '__main__':
    unittest.main()
