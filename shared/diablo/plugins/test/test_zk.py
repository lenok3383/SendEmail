"""Unittest for diablo zknode and zkft plugins.

:Authors: bharkrea, taras
$Id: //prod/main/_is/shared/python/diablo/plugins/test/test_zk.py#4 $
"""
import json
import logging
import unittest2 as unittest

import zookeeper
import zookeeper.exceptions

import shared.diablo.plugins
import shared.diablo.plugins.zkft as zkft
import shared.diablo.plugins.zknode as zknode
from shared.diablo import DiabloShutdownException
from shared.diablo.decorator_registrar import DecoratorRegistrar


# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)

class TestZKNodePlugin(unittest.TestCase):
    """Test the ZK node plugin class functionality."""

    def setUp(self):
        self._zookeeper_orig = zknode.zookeeper.ZooKeeper
        zknode.zookeeper.ZooKeeper = MockZooKeeper

    def tearDown(self):
        zknode.zookeeper.ZooKeeper = self._zookeeper_orig

    def test_run(self):
        """Test ZK node plugin run."""
        plug = zknode.ZooKeeperNodePlugin(MockDaemon(), {}, '')

        plug.startup()
        try:
            status = plug.get_status()
            self.assertEqual(status['zk_status'], 'CONNECTING_STATE')
            self.assertEqual(status['zk_session_timeout'], 0)

            node_info = plug._get_node_info()
            self.assertEqual(node_info['app_name'], 'example')
            self.assertEqual(node_info['web_port'], None)
            self.assertEqual(node_info['rpc_port'], None)

            self.assertIsInstance(plug._get_node_status_list(), list)
            self.assertIn('Node status', plug._get_web_node_status())
        finally:
            plug.shutdown()

    def test_watcher_auth_fail(self):
        """Test handling auth failed state."""
        daemon = MockDaemon()
        plug = zknode.ZooKeeperNodePlugin(daemon, {}, '')

        class _StopDaemonCalled: pass
        def fake_stop_daemon(reason=None):
            raise _StopDaemonCalled()
        daemon.stop_daemon = fake_stop_daemon

        plug.startup()
        try:

            zkc = zookeeper.constants
            class _Event:
                event_type = zkc.SESSION_EVENT
                state = zkc.AUTH_FAILED_STATE
                SESSION_EVENT = zkc.SESSION_EVENT
                AUTH_FAILED_STATE = zkc.AUTH_FAILED_STATE
                EXPIRED_SESSION_STATE = zkc.EXPIRED_SESSION_STATE
            event = _Event()

            with self.assertRaises(_StopDaemonCalled):
                plug._zk_connection_watcher(event)
        finally:
            plug.shutdown()

    def test_watcher_expire_session(self):
        """Test handling session expiration."""
        daemon = MockDaemon()
        plug = zknode.ZooKeeperNodePlugin(daemon, {}, '')

        class _CloseZookeeperCalled: pass
        def fake_close(Cls):
            raise _CloseZookeeperCalled()
        zknode.zookeeper.ZooKeeper.close = fake_close

        plug.startup()
        try:

            zkc = zookeeper.constants
            class _Event:
                event_type = zkc.SESSION_EVENT
                state = zkc.EXPIRED_SESSION_STATE
                SESSION_EVENT = zkc.SESSION_EVENT
                AUTH_FAILED_STATE = zkc.AUTH_FAILED_STATE
                EXPIRED_SESSION_STATE = zkc.EXPIRED_SESSION_STATE
            event = _Event()

            with self.assertRaises(_CloseZookeeperCalled):
                plug._zk_connection_watcher(event)
        finally:
            plug.shutdown()

    def test_node_status_list(self):
        """Test the type of node_status_list methods return."""
        plug = zknode.ZooKeeperNodePlugin(MockDaemon(), {}, '')

        def fake_get_node_status_list():
            return [('Dummy Node', ())]
        plug._get_node_status_list = fake_get_node_status_list

        plug.startup()
        try:
            self.assertIsInstance(plug.node_status_list(), list)
            self.assertIsInstance(plug.rpc_node_status_list(), list)
        finally:
            plug.shutdown()


class TestZKFTPlugin(unittest.TestCase):
    """Test the ZK FT plugin class functionality."""

    def setUp(self):
        self._zookeeper_orig = zkft.zookeeper.ZooKeeper
        zkft.zookeeper.ZooKeeper = MockZooKeeper

        self._recipes_orig = zkft.zookeeper.recipes.lock.Lock
        zkft.zookeeper.recipes.lock.Lock = MockZooLock

    def tearDown(self):
        zkft.zookeeper.ZooKeeper = self._zookeeper_orig
        zkft.zookeeper.recipes.lock.Lock = self._recipes_orig

    def test_run(self):
        """Test ZK FT plugin run."""
        plug = zkft.ZooKeeperFTPlugin(MockDaemon(), {}, '',
                                      service_names=['my_service'])

        plug.startup()
        try:
            self.assertEqual(plug.get_services(), ['my_service'])
            self.assertFalse(plug.should_start_app())
            self.assertEqual(plug.get_status()['service'], 'standing by')
            self.assertFalse(plug.safe_to_assert_master())
            # simulate the callback call
            plug._got_lock('my_service')
            # verify it
            self.assertTrue(plug.should_start_app())
            self.assertEqual(plug.get_status()['service'], 'my_service')

            status = plug.get_status()
            self.assertEqual(status['zk_status'], 'CONNECTING_STATE')
            self.assertEqual(status['zk_session_timeout'], 0)

            node_info = plug._get_node_info()
            self.assertEqual(node_info['app_name'], 'example')
            self.assertEqual(node_info['web_port'], None)
            self.assertEqual(node_info['rpc_port'], None)

            data = plug._get_node_data()
            data_dict = json.loads(data)
            self.assertEqual(data_dict['node_state'], 'my_service')

            self.assertIsInstance(plug._get_node_status_list(), list)
            self.assertIn('Service info', plug._get_web_cluster_status())
        finally:
            plug.shutdown()

    def test_watcher_events(self):
        """Test daemon shutdown is called on relevant events."""
        daemon = MockDaemon()
        plug = zkft.ZooKeeperFTPlugin(daemon, {}, '',
                                      service_names=['active'])

        class _StopDaemonCalled: pass
        def fake_stop_daemon(reason=None):
            raise _StopDaemonCalled()
        daemon.stop_daemon = fake_stop_daemon

        plug.startup()
        try:
            # simulate the callback call
            plug._got_lock('active')

            zkc = zookeeper.constants
            class _Event:
                event_type = zkc.SESSION_EVENT
                state = zkc.AUTH_FAILED_STATE
                SESSION_EVENT = zkc.SESSION_EVENT
                AUTH_FAILED_STATE = zkc.AUTH_FAILED_STATE
                EXPIRED_SESSION_STATE = zkc.EXPIRED_SESSION_STATE
            event = _Event()

            with self.assertRaises(_StopDaemonCalled):
                plug._zk_connection_watcher(event)

            event.state = zkc.EXPIRED_SESSION_STATE
            with self.assertRaises(_StopDaemonCalled):
                plug._zk_connection_watcher(event)
        finally:
            plug.shutdown()

    def test_ensure_master(self):
        """Test ensure_master returns True when it is safe to assert master."""
        plug = zkft.ZooKeeperFTPlugin(MockDaemon(), {}, '',
            service_names=['dummyService'])
        def mock_safe_to_assert_master():
            return True
        plug.safe_to_assert_master = mock_safe_to_assert_master

        plug.ensure_master()
        # If we get here, ensure_master has ensured that we are the master,
        # otherwise it would have thrown an exception.

    def test_ensure_master_false(self):
        """Test ensure_master failure conditions.

        Test that ensure master throws an exception when it is not safe to
        assert master and should_continue returns false.
        """
        mock_daemon = MockDaemon()
        mock_daemon._should_continue = False
        plug = zkft.ZooKeeperFTPlugin(mock_daemon, {}, '',
            service_names=['dummyService'])
        def mock_safe_to_assert_master():
            return False
        plug.safe_to_assert_master = mock_safe_to_assert_master

        self.assertRaises(DiabloShutdownException, plug.ensure_master)

    def test_zk_unregister_node(self):
        """Test zk_unregister fails quietly on non critical errors."""
        excs = [zookeeper.exceptions.NoNodeException,
            zookeeper.exceptions.ConnectionLossException,
            zookeeper.exceptions.ZooKeeperException]
        plug = shared.diablo.ZooKeeperNodePlugin(MockDaemon(), {}, '')
        original_delete = MockZooKeeper.delete
        for exc in excs:
            try:
                def mock_delete_error(self, path):
                    raise exc()
                MockZooKeeper.delete = mock_delete_error
                plug._zk_unregister_node()
            finally:
                MockZooKeeper.delete = original_delete


class MockDaemon(object):

    __metaclass__ = DecoratorRegistrar

    app_name = 'example'
    app_started = True
    hostname = 'vmhost01.soma.ironport.com'
    node_id = 0
    pid = 48222
    user = 'case'
    group = 'case'
    _app_start_time = 0
    _daemon_start_time = 0
    VERSION = '1.0'
    _plugins = {}

    def __init__(self):
        self._should_continue = True

    def should_continue(self):
        return self._should_continue

    def shallow_sleep(self, time):
        pass

    def get_status(self):
        return {}

    def get_daemon_uptime(self):
        return 0.0

    def get_app_uptime(self):
        return 0.0

    def stop_daemon(self, reason=None):
        pass

class MockZooKeeper(object):

    def __init__(self, *args, **kwargs):
        pass

    def get_state(self):
        return zookeeper.constants.CONNECTING_STATE

    def get_session_timeout(self):
        return 0

    def exists(self, path):
        return MockStat()

    def get_client_id(self):
        return '123', '456'

    def get(self, path):
        return {}, MockStat()

    def set(self, path, data):
        return MockStat()

    def create(self, *args, **kwargs):
        return '/diablo-1.0-mock/ut'

    def delete(self, path):
        pass

    def close(self):
        pass

    def get_children(self, path):
        return []

class MockStat(object):
    ephemeral_owner = '123'

class MockZooLock(object):
    def __init__(self, *args, **kwargs):
        pass

    def lock(self, *args, **kwargs):
        return True

    def try_cleanup(self):
        return True


if __name__ == '__main__':
    unittest.main()

