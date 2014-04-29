"""Unittest for diablo web plugin, wsgi app, and web_method decorator.

:Authors: scottrwi
$Id: //prod/main/_is/shared/python/diablo/plugins/test/test_web.py#9 $
"""

import cStringIO
import logging
import socket
import unittest2 as unittest
import urllib2
import wsgiref.simple_server

from shared.diablo.decorator_registrar import DecoratorRegistrar
from shared.diablo.plugins.web import WebPlugin, web_method, \
                                      DiabloWSGIApp, STATUS, format_uptime

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)


class TestWebPlugin(unittest.TestCase):
    """Test the plugin class functionality and the web_method decorator.
    """

    def test_run(self):
        self.plug = WebPlugin(MockDaemon(), {}, '')
        self.plug.get_app = lambda: wsgiref.simple_server.demo_app

        self.plug.startup()

        self.assertEqual(len(self.plug._methods), 2)
        self.assertFalse(self.plug._methods['web_method1'][1]) # hide
        self.assertFalse(self.plug._methods['web_method1'][2]) # direct
        self.assertFalse(self.plug._methods['method2'][1]) # hide
        self.assertTrue(self.plug._methods['method2'][2]) # direct

        try:
            resp = urllib2.urlopen('http://localhost:%d/' % (self.plug.port,),
                                   timeout=10)
            self.assertIn('Hello', resp.read())

            status = self.plug.get_status()
            self.assertIn('port', status)
        finally:
            self.plug.shutdown()

    def test_conf_port(self):
        self.plug = WebPlugin(MockDaemon(), {'diablo.web_server_port': 2000},
                              'diablo')
        self.plug.get_app = lambda: wsgiref.simple_server.demo_app
        self.plug.startup()
        try:
            status = self.plug.get_status()
            self.assertEqual(2000, status['port'])
        finally:
            self.plug.shutdown()


class TestFormatUptime(unittest.TestCase):

    def test_format_uptime(self):
        self.assertEqual(format_uptime(0), 'Not Running')
        self.assertEqual(format_uptime(72), '1 Minute, 12.0 Seconds')
        self.assertEqual(format_uptime(3600), '1 Hour, 0 Minutes, 0.0 Seconds')
        self.assertEqual(format_uptime(86400 + 120), '1 Day, 0 Hours, 2 Minutes, 0.0 Seconds')
        self.assertEqual(format_uptime(86400 * 24), '24 Days, 0 Hours, 0 Minutes, 0.0 Seconds')


class MockWebMethods(object):
    def web_regular(self, params):
        return params['foo'][0]

    def web_hidden(self, params):
        return 'web_hidden'

    def web_direct(self, params):
        return params['foo'][0]

    def web_exc(self, params):
        raise Exception()


class TestWSGIApp(unittest.TestCase):
    """Test the WSGI App.
    """

    def setUp(self):
        self.web_methods = MockWebMethods()
        # Methods dict is {<name>: (method_obj, hide, direct)}
        methods = {'web_regular': (self.web_methods.web_regular, False, False),
                   'web_hidden': (self.web_methods.web_hidden, True, False),
                   'web_direct': (self.web_methods.web_direct, False, True),
                   'web_exc': (self.web_methods.web_exc, False, False)}

        self.app = DiabloWSGIApp(methods, MockDaemon())

    def start_resp(self, status, headers):
        self.status = status
        self.headers = headers

    def assertFound(self, search, query):
        self.assertIn(query, search)

    def assertNotFound(self, search, query):
        self.assertNotIn(query, search)

    def test_reg_method(self):
        environ = {'PATH_INFO': '/web_regular',
                   'QUERY_STRING': 'foo=boo',
                   'REQUEST_METHOD': 'GET'}

        ret = self.app(environ, self.start_resp)
        ret = ret[0]
        self.assertFound(ret, 'boo')
        self.assertEqual(self.status, STATUS[200])

        # Look for link to this method
        self.assertFound(ret, '/web_regular')

        # Make sure hidden method isn't displayed
        self.assertNotFound(ret, '/web_hidden')

    def test_post(self):
        body = 'foo=boo'
        foo_fi = MockFile(body)
        environ = {'PATH_INFO': '/web_direct',
                   'wsgi.input': foo_fi,
                   'CONTENT_LENGTH': str(len(body)),
                   'REQUEST_METHOD': 'POST'}

        ret = self.app(environ, self.start_resp)
        ret = ret[0]
        self.assertFound(ret, 'boo')
        self.assertEqual(self.status, STATUS[200])

    def test_direct_method(self):
        environ = {'PATH_INFO': '/web_direct',
                   'QUERY_STRING': 'foo=boo',
                   'REQUEST_METHOD': 'GET'}

        ret = self.app(environ, self.start_resp)
        ret = ret[0]
        self.assertEqual(ret, 'boo')
        self.assertEqual(self.status, STATUS[200])

    def test_exception_method(self):
        environ = {'PATH_INFO': '/web_exc',
                   'QUERY_STRING': '',
                   'REQUEST_METHOD': 'GET'}

        ret = self.app(environ, self.start_resp)
        ret = ret[0]
        self.assertFound(ret, 'Traceback')
        self.assertEqual(self.status, STATUS[500])

    def test_unknown_method(self):
        environ = {'PATH_INFO': '/unknown_method',
                   'QUERY_STRING': '',
                   'REQUEST_METHOD': 'GET'}

        ret = self.app(environ, self.start_resp)
        ret = ret[0]
        self.assertFound(ret, 'unknown_method')
        self.assertEqual(self.status, STATUS[404])

    def test_root_redirect(self):
        environ = {'PATH_INFO': '/',
                   'QUERY_STRING': '',
                   'REQUEST_METHOD': 'GET'}

        ret = self.app(environ, self.start_resp)
        self.assertEqual(self.status, STATUS[301])
        self.assertEqual(dict(self.headers)['Location'], '/status')

    def test_all_plugins(self):
        methods = {'web_regular': (self.web_methods.web_regular, False, False)}
        self.app = DiabloWSGIApp(methods, MockDaemonAllPlugins())

        environ = {'PATH_INFO': '/web_regular',
                   'QUERY_STRING': 'foo=boo',
                   'REQUEST_METHOD': 'GET'}

        ret = self.app(environ, self.start_resp)
        ret = ret[0]
        self.assertFound(ret, 'Backdoor Port')
        self.assertFound(ret, 'RPC Port')


class MockFile(object):

    def __init__(self, content):
        self.content = content

    def read(self, length):
        return self.content[:length]


class MockDaemon(object):

    __metaclass__ = DecoratorRegistrar

    app_name = 'example'
    app_started = True
    hostname = 'vmhost01.soma.ironport.com'
    node_id = 0
    pid = 48222
    VERSION = '1.0'
    _plugins = []

    def get_status(self):
        return {'plugins': {'web': {'port': 22080}}}

    def get_daemon_uptime(self):
        return 0.0

    def get_app_uptime(self):
        return 0.0

    def stop_daemon(self, reason=None):
        pass

    @web_method
    def web_method1(self, params):
        pass

    @web_method(name='method2', hide=False, direct=True)
    def web_method2(self, params):
        pass


class MockDaemonAllPlugins(MockDaemon):

    def get_status(self):
        ret = {}
        ret['plugins'] = {'rpc': {'methods': ['hello', 'status', 'restart'],
                                  'port': 22000},
                          'web': {'port': 22080},
                          'backdoor': {'port': 22050}}
        return ret


if __name__ == '__main__':
    unittest.main()
