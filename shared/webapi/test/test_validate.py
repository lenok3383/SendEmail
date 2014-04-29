"""Unit tests for Web API validate module.

:Status: $Id: //prod/main/_is/shared/python/webapi/test/test_validate.py#5 $
:Authors: ohmelevs
"""

import doctest
import unittest2 as unittest

from shared.webapi import validate


class TestValidate(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_validate(self):
        doctest.testmod(validate, verbose=False)


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestValidate))
    runner = unittest.TextTestRunner()
    runner.run(suite)
