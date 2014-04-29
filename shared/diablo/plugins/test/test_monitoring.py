"""Unittest for monitoring plugin.

:Authors: taras, nkit
"""
import json
import logging

import unittest2 as unittest
import shared.diablo.plugins.monitoring as monitor


#Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)

class TestMonitorPlugin(unittest.TestCase):
    """Tests for MonitorPlugin class functionality."""

    def setUp(self):
        self._gen_counters_data_orig = monitor._gen_counters_data
        self._gen_nodes_status_orig = monitor._gen_nodes_status

        self._monitor = monitor
        self._monitor_plug = monitor.MonitorPlugin(MockDaemon(), {}, '')

    def tearDown(self):
        monitor._gen_counters_data = self._gen_counters_data_orig
        monitor._gen_nodes_status = self._gen_nodes_status_orig

    def test_web_counters_json(self):
        # No counters
        self._monitor._gen_counters_data = lambda: None
        result = self._monitor_plug.web_counters_json({})
        self.assertEqual(result, '37a6259cc0c1dae299a7866489dff0bd\nnull')

        # Was generated correct counters data
        data = {'publishing': {
             'publishing:all_targets_failed': {'average rate': '0.0000',
                                               'current rate': '0.0000',
                                               'current value': 0,
                                               'type': 'DynamicCounter'}}}

        self._monitor._gen_counters_data = lambda:data
        #Skip md5 checksum
        result = self._monitor_plug.web_counters_json({}).split('\n')[-1]
        expected_result = json.dumps(data)
        self.assertEqual(result, expected_result)

        #Was raised error
        self._monitor._gen_counters_data = lambda: raise_(Exception())
        result = self._monitor_plug.web_counters_json({})
        self.assertEqual(result, '\n')

    def test_web_status_json(self):
        # No data
        self._monitor_plug.daemon_obj.get_status = lambda: None
        result = self._monitor_plug.web_status_json({})
        self.assertEqual(result, '37a6259cc0c1dae299a7866489dff0bd\nnull')

        # Was generated correct counters data
        data = {'app': {},
               'app_name': 'publishing',
               'app_start_time': None,
               'app_started': False,
               'app_uptime': 0.0,
               'daemon_start_time': 1366270426.535722,
               'daemon_uptime': 3945.5055310726166,
               'group': '3902',
               'hostname': 'dev-uridbgen-app1.vega.ironport.com',
               'node_id': 0,
               'pid': 50237,
               'plugins':{},
               'user': 'nkit',
               'version': '@VERSION@'}

        self._monitor_plug.daemon_obj.get_status = lambda:data
        #Skip md5 checksum
        result = self._monitor_plug.web_status_json({}).split('\n')[-1]
        expected_result = json.dumps(data)
        self.assertEqual(result, expected_result)

        #Was raised error
        self._monitor_plug.daemon_obj.get_status = lambda: raise_(Exception())
        result = self._monitor_plug.web_status_json({})
        self.assertEqual(result, '\n')

    def test_node_status_json(self):
        # No data
        self._monitor_plug.daemon_obj.node_status_list = lambda: []
        self._monitor._gen_nodes_status = lambda input_arg: None
        result = self._monitor_plug.node_status_json({})
        self.assertEqual(result, '37a6259cc0c1dae299a7866489dff0bd\nnull')

        # Was generated correct counters data
        data =  {'dev-uridbgen-app1.vega.ironport.com:12100': {
                'app': 'packaged',
                'env': 'dev/case_pkg/nkit',
                'hostname': 'dev-uridbgen-app1.vega.ironport.com',
                'link': 'dev-uridbgen-app1.vega.ironport.com:12100',
                'node_id': 0,
                'node_state': 'standby',
                'pid': 61777,
                'start_time': 'Thu Apr 18 02:24:38 2013'},}

        self._monitor._gen_nodes_status = lambda input_arg: data
        #Skip md5 checksum
        result = self._monitor_plug.node_status_json({}).split('\n')[-1]
        expected_result = json.dumps(data)
        self.assertEqual(result, expected_result)

        #Was raised error
        self._monitor_plug.daemon_obj.get_status = lambda: raise_exc(Exception())
        result = self._monitor_plug.web_status_json({})
        self.assertEqual(result, '\n')


class TestMonitoring(unittest.TestCase):
    """Tests for additional methods in monitoring plugin."""

    def setUp(self):
        self._counter_orig = monitor.counter
        self._monitor = monitor

    def tearDown(self):
        monitor.counter = self._counter_orig

    def test_gen_node_status(self):
        #Empty input args.
        result = self._monitor._gen_nodes_status([])
        self.assertEqual(result, {})
        #Correct input args
        input_data = [{'app': 'packaged',
                      'env': 'dev/case_pkg/nkit',
                      'hostname': 'dev-uridbgen-app1.vega.ironport.com',
                      'link': 'dev-uridbgen-app1.vega.ironport.com:12100',
                      'node_id': 0,
                      'node_state': 'standby',
                      'pid': 61777,
                      'web_port': 12300,
                      'start_time': 'Thu Apr 18 02:24:38 2013'},]

        expected_result = {'dev-uridbgen-app1.vega.ironport.com:12300':
                              {'node_id': 0,
                              'link': 'dev-uridbgen-app1.vega.ironport.com:12100',
                              'web_port': 12300,
                              'env': 'dev/case_pkg/nkit',
                              'node_state': 'standby',
                              'app': 'packaged',
                              'hostname': 'dev-uridbgen-app1.vega.ironport.com',
                              'pid': 61777,
                              'start_time': 'Thu Apr 18 02:24:38 2013'}}

        result = self._monitor._gen_nodes_status(input_data)
        self.assertEqual(result, expected_result)

    def test_gen_counters_data(self):
        components = []
        DynamicCounter = MockSCounter('DynamicCounter', 0.00, [0.0000, 0.0000])
        StaticCounter = MockSCounter('StaticCounter', 0.00, [0.0000, 0.0000])
        components.append(StaticCounter)
        components.append(DynamicCounter)
        self._monitor.counter = MockCounter(components)

        expected_result = {DynamicCounter: {'DynamicCounter':
                                     {'current value': 0.0,
                                      'current rate': '0.0000',
                                      'type': 'MockSCounter',
                                      'average rate': '0.0000'}},
                           StaticCounter: {'StaticCounter':
                                     {'current value': 0.0,
                                      'current rate': 'N/A',
                                      'type': 'MockSCounter',
                                      'average rate': 'N/A'}}}
        result = self._monitor._gen_counters_data()
        self.assertEqual(result, expected_result)


class MockDaemon(object):

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


class MockCounter(object):

    def __init__(self, items):
        self._items = items

    def get_sections(self):
        return [section for section in self._items]

    def get_counters_for_section(self, section):
        return [item for item in self._items if item == section]


class MockSCounter():

    def __init__(self, type, value, rate):
        self.name = type
        self.value = value
        self._rate = rate

    @property
    def rate(self):
        if self.name == 'StaticCounter':
            raise AttributeError()
        return self._rate

    def __repr__(self):
        return  self.name


def raise_exc(exc):
    raise exc


if __name__ == '__main__':
    unittest.main()
