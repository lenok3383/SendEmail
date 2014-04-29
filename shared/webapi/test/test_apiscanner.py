"""Unit tests for Web API apiscanner module.

:Status: $Id: //prod/main/_is/shared/python/webapi/test/test_apiscanner.py#16 $
:Authors: ohmelevs
"""

import cjson
import os
import shutil
import sys
import tempfile
import unittest2 as unittest

from shared.util.encoding import rencode
from shared.webapi import apiscanner
from shared.webapi.errors import NotFoundError, InconsistencyMappingError


mockup_environ = {'REMOTE_USER':'test',
                  'REQUEST_METHOD': 'GET',
                  'SCRIPT_NAME': '/version/method_name',
                  'http_user': 'test',
                  'wsgi.input': 'test',
                  'webapi_data_format': 'json',
}


mockup_startresponse = lambda x, y: True

class Test(unittest.TestCase):

    def setUp(self):
        self.test_data = 'good_test'
        self.version = 'v1'

        # Escaping of the wsgi request handling.
        self.parse_formvars = apiscanner.paste.request.parse_formvars
        apiscanner.paste.request.parse_formvars = lambda env: env

        self.mockup_environ = mockup_environ.copy()

        api_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), self.test_data))

        # Yuck! apiscanner implcitly requires the api_path to be in sys.path.
        sys.path.insert(0, api_path)
        self.addCleanup(sys.path.pop, 0)

        self.instance = apiscanner.ServiceExposer(api_path)

    def tearDown(self):
        apiscanner.paste.request.parse_formvars = self.parse_formvars
        self.mockup_environ = None
        # We don't want to spoil imports of another test case.
        if 'v1' in sys.modules:
            del sys.modules['v1']
        if 'v2' in sys.modules:
            del sys.modules['v2']

    def test_versions(self):
        version_dict = self.instance.versions()
        self.assertTrue(isinstance(version_dict, dict) and
                        self.version in version_dict['versions'])

    def test_wrong_resource(self):
        self.mockup_environ['SCRIPT_NAME'] = '/%s/%s' % (self.version, 'wrong')

        result = self.instance(self.mockup_environ, mockup_startresponse)[0]
        result_dict = cjson.decode(result)
        self.assertItemsEqual(result_dict, ('ERR_STR', 'ERR_CODE'))

        # Some versions of cjson encode / as \\/, so we need to be lenient
        # here.
        self.assertRegexpMatches(
            result_dict['ERR_STR'],
            r'Resource (?:\\)?/v1(?:\\)?/wrong not found')
        self.assertEqual(result_dict['ERR_CODE'], '404 Not found')

    def test_wrong_version(self):
        self.mockup_environ['SCRIPT_NAME'] = '/%s/%s' % ('v11', 'test1')

        result = self.instance(self.mockup_environ, mockup_startresponse)[0]
        result_dict = cjson.decode(result)
        self.assertItemsEqual(result_dict, ('ERR_STR', 'ERR_CODE'))

        # Some versions of cjson encode / as \\/, so we need to be lenient
        # here.
        self.assertRegexpMatches(
            result_dict['ERR_STR'],
            r'Resource (?:\\)?/v11(?:\\)?/test1 not found')
        self.assertEqual(result_dict['ERR_CODE'], '404 Not found')

    def test_execution_two_modules(self):
        self.mockup_environ['SCRIPT_NAME'] = '/%s/%s' % ('v1', 'test1')
        self.instance(self.mockup_environ, mockup_startresponse)
        self.mockup_environ['SCRIPT_NAME'] = '/%s/%s' % ('v1', 'test2')
        self.instance(self.mockup_environ, mockup_startresponse)

    def test_execution_second_version(self):
        self.mockup_environ['SCRIPT_NAME'] = '/%s/%s' % ('v12', 'test1')
        result = cjson.decode(self.instance(self.mockup_environ,
                                            mockup_startresponse)[0])
        self.assertTrue(result == 84)

    def test_execution_json(self):
        self.mockup_environ['SCRIPT_NAME'] = '/%s/%s' % (self.version, 'test1')

        result = cjson.decode(self.instance(self.mockup_environ,
                                            mockup_startresponse)[0])
        self.assertTrue(result == 42)

    def test_execution_rencode(self):
        self.mockup_environ['SCRIPT_NAME'] = '/%s/%s' % (self.version, 'test1')

        self.mockup_environ['webapi_data_format'] = 'rencode'
        result = self.instance(self.mockup_environ, mockup_startresponse)[0]
        self.assertTrue(rencode.loads(result) == 42)

    def test_execution_url_param(self):
        self.mockup_environ['SCRIPT_NAME'] = '/%s/%s/%s/%s' % (self.version,
                                                            'test2',
                                                            'test_method1',
                                                            '42')
        result = cjson.decode(self.instance(self.mockup_environ,
                                            mockup_startresponse)[0])
        self.assertTrue(result == 42)

    def test_execution_query_param_default(self):
        self.mockup_environ['SCRIPT_NAME'] = '/%s/%s/%s' % (self.version,
                                                            'test2',
                                                            'test_method2')
        result = cjson.decode(self.instance(self.mockup_environ,
                                            mockup_startresponse)[0])
        self.assertTrue(result is None)

    def test_execution_query_param(self):
        self.mockup_environ['SCRIPT_NAME'] = '/%s/%s/%s' % (self.version,
                                                            'test2',
                                                            'test_method2')

        self.mockup_environ['echo_str'] = '"ECHO!"'
        result = cjson.decode(self.instance(self.mockup_environ,
                                            mockup_startresponse)[0])
        self.assertTrue(result == 'ECHO!')

    def test_rencode(self):
        test_value = 1.69
        self.assertAlmostEqual(test_value,
                               rencode.loads(rencode.dumps(test_value)), 3)
        test_value = {'string': [None, 0, 1]}
        self.assertEqual(test_value, rencode.loads(rencode.dumps(test_value)))

    def test_get_spec(self):
        self.assertRaises(NotFoundError, self.instance.get_spec,
                          'wrong_version')
        spec = self.instance.get_spec(self.version)
        self.assertTrue(isinstance(spec, str))
        self.assertTrue('echo_str' in spec)
        self.assertTrue('url_int' in spec)
        self.assertTrue(':URL: /v1/test1' in spec)
        self.assertTrue(':URL: /v1/test2/test_method1' in spec)
        self.assertTrue(':URL: /v1/test2/test_method2' in spec)
        self.assertEqual(spec.count('Test method'), 3)

    def test_get_help(self):
        self.assertRaises(NotFoundError, self.instance.get_help,
                          'wrong_version')
        self.assertEqual(self.instance.get_help(self.version)[-1],
                         self.version)
        api_help = self.instance.get_help(self.version)[0]
        self.assertTrue(isinstance(api_help, dict))
        functions_dict = api_help['']
        self.assertTrue(isinstance(functions_dict, dict))
        self.assertTrue(functions_dict.has_key('test1.test_method1'))
        self.assertTrue(functions_dict.has_key('test2.test_method1'))
        self.assertTrue(functions_dict.has_key('test2.test_method2'))

    def test_api_meta_data(self):
        self.assertRaises(NotFoundError, self.instance.api_meta_data,
                          'wrong_version')
        self.assertEqual(self.instance.api_meta_data(self.version)[-1],
                         self.version)
        meta_data = self.instance.api_meta_data(self.version)[0]
        self.assertTrue(isinstance(meta_data, dict))
        self.assertTrue(meta_data.has_key('test1.test_method1'))
        self.assertTrue(meta_data.has_key('test2.test_method1'))
        self.assertTrue(meta_data.has_key('test2.test_method2'))

        func = meta_data['test1.test_method1']
        self.assertTrue(isinstance(func, dict))
        self.assertTrue(func.has_key('QUERY_PARAMS'))
        self.assertTrue(func.has_key('URL_PARAMS'))
        self.assertTrue(func.has_key('HTTP_METHOD'))
        self.assertTrue(func.has_key('HTTP_URL'))


class TestInconsistencyMapping(unittest.TestCase):

    def setUp(self):
        self.test_data = 'bad_test'
        self.version = 'v2'

        # Escaping of the wsgi request handling.
        self.parse_formvars = apiscanner.paste.request.parse_formvars
        apiscanner.paste.request.parse_formvars = lambda env: env

        self.mockup_environ = mockup_environ.copy()

        self.api_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), self.test_data))

        # Yuck! apiscanner implcitly requires the api_path to be in sys.path.
        sys.path.insert(0, self.api_path)
        self.addCleanup(sys.path.pop, 0)

    def tearDown(self):
        apiscanner.paste.request.parse_formvars = self.parse_formvars
        self.mockup_environ = None
        if 'v2' in sys.modules:
            del sys.modules['v2']

    def test_incostistence_mapping(self):
        self.assertRaises(InconsistencyMappingError,
                          apiscanner.ServiceExposer, self.api_path)


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test))
    suite.addTest(unittest.makeSuite(TestInconsistencyMapping))
    runner = unittest.TextTestRunner()
    runner.run(suite)
