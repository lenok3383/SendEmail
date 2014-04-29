"""Test the registrar module.

:Status: $Id: //prod/main/_is/shared/python/net/test/test_registrar.py#3 $
:Authors: vburenin, yifenliu
"""

import unittest2 as unittest

from shared.net import registrar


class IputilsTestCase(unittest.TestCase):

    def setUp(self):
        registrar.init_registrar()
        self.reg = registrar.get_registrar()

    def test_is_tld(self):
        self.assertEqual(False, self.reg.is_tld('vasya.com'))
        self.assertEqual(True, self.reg.is_tld('pvt.k12.ca.us'))
        self.assertEqual(True, self.reg.is_tld('com'))

    def test_trim_domain_and_normalization(self):
        self.assertEqual('tt.pvt.k12.ca.us',
                         self.reg.trim_domain('  www.tt.pvt.k12.ca...us '))

    def test_split_domain(self):
        self.assertEqual(('www', 'kylev.com'),
                         self.reg.split_domain('www.kylev.com'))
        self.assertEqual(('www', 'foo.co.uk'),
                         self.reg.split_domain('www.foo.co.uk'))
        self.assertEqual(('www', 'blah.org.vi'),
                         self.reg.split_domain('www.blah.org.vi'))
        self.assertEqual(('', 'kylev.com'),
                         self.reg.split_domain('kylev.com'))
        self.assertEqual(('', 'com'),
                         self.reg.split_domain('com'))
        self.assertEqual(('', 'www.loki.ca.us'),
                         self.reg.split_domain('www.loki.ca.us'))
        self.assertEqual(('www.test', 'goedod.depopdk'),
                         self.reg.split_domain('www.test.goedod.depopdk'))
        self.assertEqual(('www.koki', 'vasya.ru'),
                         self.reg.split_domain('www.koki.vasya.ru'))
        self.assertEqual(('www', 'fire-dept.ci.los-angeles.or.us'),
                         self.reg.split_domain(
                                '   www.Fire-Dept.CI.Los-Angeles...OR.US\n'))
        self.assertEqual(('www', 'blah.esc.edu.ar'),
                         self.reg.split_domain('www.blah.esc.edu.ar'))
        self.assertEqual(('www', 'blah.demon.co.uk'),
                         self.reg.split_domain('www.blah...demon.co.uk'))
        self.assertEqual(('', 'dom.lkd.co.im'),
                         self.reg.split_domain('dom.lkd.co.im'))

    def test_is_domain_valid(self):
        self.assertEqual(True, self.reg.is_domain_valid('12306.com'))
        self.assertEqual(True, self.reg.is_domain_valid('en.wikipedia.org'))
        self.assertEqual(True, self.reg.is_domain_valid('test-test.ac'))
        self.assertEqual(False, self.reg.is_domain_valid('test_test.com'))
        self.assertEqual(False, self.reg.is_domain_valid('9/7/2000.ru'))
        self.assertEqual(False, self.reg.is_domain_valid('withvodka.com.au.tc:81'))


if __name__ == '__main__':
    unittest.main()
