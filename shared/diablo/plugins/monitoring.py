"""MonitorPlugin adds monitoring to Diablo node.

Plugin for Diablo that contains methods for checking correct work
of a Diablo application. The methods it supports are web methods:
counters.json, status.json, node_status.json.

Requested by MonOps: https://confluence.sco.cisco.com/x/EYF1 (Monitoring
services which use Diablo)

:Authors: taras, nkit
"""

import hashlib
import json
import logging

from shared.diablo.plugins import DiabloPlugin
from shared.diablo.plugins.web import web_method
from shared.monitor import counter


class MonitorPlugin(DiabloPlugin):
    """Plugin for Diablo that contains methods for monitoring.

    To enable this plugin put this line in your Diablo application __init__:

    self.register_plugin(shared.diablo.plugins.monitoring.MonitorPlugin)

    Please make sure web plugin is registered for correct work
    
    of the web methods:

    self.register_plugin(shared.diablo.plugins.web.WebPlugin)

    You can use web methods(counters.json, status.json, node_status.json)
    in your application.
    """
    NAME = 'monitor'

    def __init__(self, *args, **kwargs):
        """Initialize methods list."""

        super(MonitorPlugin, self).__init__(*args, **kwargs)
        self.__log = logging.getLogger('diablo.plugins.monitoring')

    @web_method(name='counters.json', hide=True, direct=True)
    def web_counters_json(self, params):
        """Return counters information.

        First line represents md5 checksum of data in next line.
        Second line is counters data in json format.

        Example:

        8470a2114602ac6a1cd8a4fdb097d2b2\n
        {'publishing': {
             'publishing:all_targets_failed': {'average rate': '0.0000',
                                               'current rate': '0.0000',
                                               'current value': 0,
                                               'type': 'DynamicCounter'},
             'publishing:targets_attempted': {'average rate': '0.0000',
                                              'current rate': '0.0000',
                                              'current value': 0,
                                              'type': 'DynamicCounter'},
             'publishing:targets_failed': {'average rate': '0.0000',
                                           'current rate': '0.0000',
                                           'current value': 0,
                                           'type': 'DynamicCounter'},
             'publishing:targets_published': {'average rate': '0.0000',
                                              'current rate': '0.0000',
                                              'current value': 0,
                                              'type': 'DynamicCounter'}}}
        """
        try:
            counter_data = json.dumps(_gen_counters_data())
            md5s = hashlib.md5(counter_data).hexdigest()
            return '\n'.join([md5s, counter_data])
        except Exception:
            self.__log.exception('Error in counters.json')
            return '\n'

    @web_method(name='status.json', hide=True, direct=True)
    def web_status_json(self, params):
        """Return application status information.

        First line represents md5 checksum of data in next line.
        Second line is application status data in json format.

        Example:

        810677a8f60234a9a8eb9ba3c1695f92\n
        {'app': {},
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
         'plugins': {'rpc': {'methods': [
                                'status', 'node_status_list', 'restart'],
                             'port': 11300},
                     'web': {'methods': ['status',
                                         'ft_status',
                                         'counter.json',
                                         'counters.json',
                                         'publishing_status',
                                         'log_level',
                                         'node_status.json',
                                         'node_status',
                                         'nodes_connected',
                                         'status.json',
                                         'restart',
                                         'counters'],
                             'port': 12300},
                     'zkft': {'service': 'standing by',
                              'zk_session_timeout': 40000,
                              'zk_status': 'CONNECTED_STATE'}},
         'user': 'nkit',
         'version': '@VERSION@'}
        """
        try:
            status_data = json.dumps(self.daemon_obj.get_status())
            md5s = hashlib.md5(status_data).hexdigest()
            return '\n'.join([md5s, status_data])
        except Exception:
            self.__log.exception('Error in status.json')
            return '\n'

    @web_method(name='node_status.json', hide=True, direct=True)
    def node_status_json(self, params):
        """Return node status information.

        First line represents md5 checksum of data in next line.
        Second line represent status information of connected nodes
        in json format.

        Example:

        e0d9cc381d8d51bcaba4b174280f33c1\n
        {'dev-uridbgen-app1.vega.ironport.com:12100': {
            'app': 'packaged',
            'env': 'dev/case_pkg/nkit',
            'hostname': 'dev-uridbgen-app1.vega.ironport.com',
            'link': 'dev-uridbgen-app1.vega.ironport.com:12100',
            'node_id': 0,
            'node_state': 'standby',
            'pid': 61777,
            'start_time': 'Thu Apr 18 02:24:38 2013'},
         'dev-uridbgen-app1.vega.ironport.com:12101': {
            'app': 'packaged',
            'env': 'dev/case_pkg/nkit',
            'hostname': 'dev-uridbgen-app1.vega.ironport.com',
            'link': 'dev-uridbgen-app1.vega.ironport.com:12101',
            'node_id': 1,
            'node_state': 'active',
            'pid': 61779,
            'start_time': 'Thu Apr 18 02:24:38 2013'},
         'dev-uridbgen-app1.vega.ironport.com:12102': {
            'app': 'packaged',
            'env': 'dev/case_pkg/nkit',
            'hostname': 'dev-uridbgen-app1.vega.ironport.com',
            'link': 'dev-uridbgen-app1.vega.ironport.com:12102',
            'node_id': 2,
            'node_state': 'standby',
            'pid': 61778,
            'start_time': 'Thu Apr 18 02:24:38 2013'}}
        """
        try:
            nodes_data = self.daemon_obj.node_status_list()
            nodes_status = json.dumps(_gen_nodes_status(nodes_data))
            md5s = hashlib.md5(nodes_status).hexdigest()
            return '\n'.join([md5s, nodes_status])
        except Exception:
            self.__log.exception('Error in node_status.json')
            return '\n'


def _gen_nodes_status(nodes_data):
    """Generate node status data.

    Structure has the next format:
    {
       "node1_hostname:port": {
         "env": "environment identifier",
         "app": "application name",
         "hostname": "node1 hostname",
         "node_id": numeric_id,
         "node_state": "standby for passive node, application name for active",
         "start_time": "string with application start datetime",
         "pid": pid_of_application,
         "link": "hostname:port where diablo web plugin listen for http requests"
    }, ...}
    """
    nodes_status = dict()
    for node in nodes_data:
        key = '%s:%s' % (node['hostname'], node['web_port'],)
        nodes_status[key] = node
    return nodes_status


def _gen_counters_data():
    """Generate counters data.

    Structure has the next format:
    {
        "component1": {
         "component1:counter1": {
          "type": "<counter_type>",
          "current value": <counter_value>,
          "current rate": <counter_rate>,
          "average rate": <counter_average_rate>
        },} ...}
    """
    data = dict()
    for section in counter.get_sections():
        sect_dict = dict()
        for s_counter in counter.get_counters_for_section(section):
            counter_dict = dict()
            counter_dict['type'] = s_counter.__class__.__name__
            counter_dict['current value'] = s_counter.value
            try:
                counter_dict['current rate'] = '%0.4f' % s_counter.rate[0]
            except AttributeError:
                #Static counter hasn't property rate
                counter_dict['current rate'] = 'N/A'
            try:
                counter_dict['average rate'] = '%0.4f' % s_counter.rate[1]
            except AttributeError:
                #Static counter hasn't property rate
                counter_dict['average rate'] = 'N/A'

            sect_dict[s_counter.name] = counter_dict
        data[section] = sect_dict
    return data
