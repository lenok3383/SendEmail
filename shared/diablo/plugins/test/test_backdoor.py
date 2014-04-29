"""Unittest for diablo web plugin, wsgi app, and web_method decorator.

:Authors: scottrwi
$Id: //prod/main/_is/shared/python/diablo/plugins/test/test_backdoor.py#6 $
"""

import logging
import unittest2 as unittest

from shared.diablo.plugins import backdoor

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)


class TestBackdoorPlugin(unittest.TestCase):
    """Test the plugin class functionality and the rpc_method decorator.
    """

    def test_run(self):
        self.addCleanup(setattr, backdoor, 'BackdoorServer',
                        backdoor.BackdoorServer)
        backdoor.BackdoorServer = MockServer

        self.plug = backdoor.BackdoorPlugin(MockDaemon(), {}, '')

        self.plug.startup()

        status = self.plug.get_status()
        self.assertIn('port', status)

        self.plug.shutdown()


class MockServer(object):
    def __init__(self, path, locals, welcome):
        pass

    def serve_forever(self):
        pass

    def shutdown(self):
        pass

    def server_close(self):
        pass


class MockDaemon(object):

    node_id = 0
    app_name = 'test'
    pid = 12345

    def stop_daemon(self, reason=None):
        pass


if __name__ == '__main__':
    unittest.main()

