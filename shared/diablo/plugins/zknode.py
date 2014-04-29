"""ZooKeeper Node for Diablo.

Plugin for Diablo that runs a ZooKeeper client to enable basic node
statistics to be stored in ZooKeeper.

:Author: taras, duncan
$Id: //prod/main/_is/shared/python/diablo/plugins/zknode.py#9 $
"""
import json
import logging
import operator
import os
import time

import zookeeper
import zookeeper.helpers

import shared
from shared.diablo import rpc_method, web_method
from shared.diablo.plugins import public_method
from shared.diablo.plugins import DiabloPlugin
from shared.testing.dummy_zookeeper import DummyZooKeeperConnection

class ZooKeeperNodePlugin(DiabloPlugin):

    """Plugin for Diablo that runs a ZooKeeper client to enable basic node
    statistics in ZooKeeper.

    To enable this plugin put this line in your Diablo application __init__:

    self.register_plugin(shared.diablo.plugins.zknode.ZooKeeperNodePlugin)

    Configuration:
        conf_section.zookeeper_env_product_name, default 'dev/<nicename>'
          Where, <nicename> is the last path segment from
          shared.conf.env.get_prod_root() using os.sep as a separator symbol.
          For /usr/local/ironport/whiskey the default will be 'dev/whiskey'.
          A warning message is shown during initialization of the plugin in
          case the default self-generated value is used.

        conf_section.zookeeper_conn_string, default '127.0.0.1:2181'

        conf_section.zookeeper_dummy_connection, default False
          Setting this variable will cause the plugin to use the dummy connection
          class instead of actually connecting to the ZooKeeper server.  The
          use of this variable is not typical.
    """

    NAME = 'zknode'

    def __init__(self, *args, **kwargs):
        """Initialize instance variables."""
        self.__log = logging.getLogger('diablo.plugins.zknode')

        super(ZooKeeperNodePlugin, self).__init__(*args, **kwargs)

        self.env_name = self.conf.get(
            '%s.zookeeper_env_product_name' % (self.conf_section,))
        if self.env_name is None:
            nicename = shared.conf.env.get_prod_root().split(os.sep)[-1]
            self.env_name = 'dev/%s' % (nicename,)
            self.__log.warn('Config var zookeeper_env_product_name '
                            'is not set, using self-generated %s.',
                            self.env_name)

        self.conn_string = self.conf.get(
            '%s.zookeeper_conn_string' % (self.conf_section,),
            '127.0.0.1:2181')

        self.use_dummy_connection = self.conf.get(
            '%s.zookeeper_dummy_connection' % (self.conf_section,),
            False)

        self._unique_id = '_'.join(
            (self.daemon_obj.hostname,
             str(self.daemon_obj.node_id),
             str(self.daemon_obj.pid)))

        self._acls = zookeeper.ACL_OPEN_ACL_UNSAFE

        self._zookeeper = self._init_zookeeper()

        self._namespace = '/diablo-1.0/%s' % (self.env_name,)
        self._node_status_dir = '%s/node_status' % (self._namespace,)
        self._node_app_status_dir = '%s/%s' % (
            self._node_status_dir, self.daemon_obj.app_name,)
        self._my_node = '%s/%s' % (
            self._node_app_status_dir, self._unique_id,)

    def startup(self):
        """Start the zknode plugin."""

        self.__log.info('Starting up ZK Node plugin.')
        self._zk_register()

    def get_status(self):
        """Return a dictionary of status information."""
        status = {}
        status['zk_status'] = zookeeper.Event.STATES[
            self._zookeeper.get_state()]
        status['zk_session_timeout'] = self._zookeeper.get_session_timeout()
        return status

    def shutdown(self):
        """Stop the ZooKeeper client."""
        self.__log.info('Shutting down ZK Node plugin.')
        return self._zk_unregister_node()

    def _init_zookeeper(self):
        """Creates a ZooKeeper connection."""
        if not self.use_dummy_connection:
            return zookeeper.ZooKeeper(self.conn_string, 60 * 1000,
                                       watcher=self._zk_connection_watcher)
        else:
            return DummyZooKeeperConnection(self.conn_string, 60 * 1000)

    def _zk_connection_watcher(self, event):
        """Watches the ZooKeeper connection.

        On expiration, we create a new session and re-register our nodes.
        Subclasses may not want this behavior.
        """
        assert event.event_type == event.SESSION_EVENT
        if event.state == event.EXPIRED_SESSION_STATE:
            self.__log.warn('ZooKeeper connection expired, re-creating.')
            old_zookeeper = self._zookeeper
            self._zookeeper = self._init_zookeeper()
            self._zk_register()
            try:
                old_zookeeper.close()
            except zookeeper.exceptions.ZooKeeperException:
                self.__log.warn('Error closing expired ZooKeeper',
                                exc_info=True)

        elif event.state == event.AUTH_FAILED_STATE:
            self.daemon_obj.stop_daemon('authentication failed connecting '
                                        'to ZooKeeper.')

    def _zk_register(self):
        try:
            zookeeper.helpers.ensure_exists(
                self._zookeeper,
                self._node_app_status_dir)
            self._zk_set_node()
        except zookeeper.exceptions.ConnectionLossException:
            self.daemon_obj.stop_daemon('cannot connect to your '
                                        'ZooKeeper: %s' % self.conn_string)

    @zookeeper.helpers.retry()
    def _zk_set_node(self):
        self.__log.info('Registering node status for %s', self._my_node)
        packed_data = json.dumps(self._get_node_info())

        stat = self._zookeeper.exists(self._my_node)
        if stat:
            if stat.ephemeral_owner != self._zookeeper.get_client_id()[0]:
                self.__log.warn(
                    'Somebody else owns my ZooKeeper node: %s',
                    self._my_node)
                self.daemon_obj.stop_daemon('somebody owned my ZooKeeper '
                                            'node: %s' % (self._my_node,))
                return

            self._zookeeper.set(self._my_node, packed_data)
            self.__log.info('Updated node.')
        else:
            self._zookeeper.create(self._my_node, packed_data,
                                   self._acls, ephemeral=True)
            self.__log.info('Created node.')

    def _zk_unregister_node(self):
        # No retry, if we have a connection loss error, who cares, it's
        # ephemeral, it'll go away (eventually) after we exit.
        try:
            self._zookeeper.delete(self._my_node)
        except zookeeper.exceptions.NoNodeException:
            self.__log.warn('No ZK node for deletion found: %s',
                            self._my_node)
        except zookeeper.exceptions.ConnectionLossException:
            self.__log.info('Lost connection while deleting ZK node: %s',
                            self._my_node)
        except zookeeper.exceptions.ZooKeeperException, e:
            self.__log.warn('ZK Exception deleting ZK node: %s', e)
        except Exception, e:
            self.__log.warn('Exception deleting ZK node: %s', e)

    @web_method(name='node_status')
    def web_node_status(self, *args, **kwargs):
        """Display node status information."""
        return self._get_web_node_status()

    @zookeeper.helpers.retry()
    def _get_web_node_status(self):
        table_header = ['env', 'app', 'hostname', 'node_id', 'node_state',
                        'start_time', 'pid', 'link',]
        table_data = []
        section_head = '<h1>Node status</h1>'
        try:
            for (data, zstat) in self._get_node_status_list():
                table_data.append(self._parse_row(data, zstat))

            # Sort -> hostname -> node_id -> node_state -> app -> env.
            table_data = sorted(table_data, key=operator.itemgetter(2))
            table_data = sorted(table_data, key=operator.itemgetter(3))
            table_data = sorted(table_data, key=operator.itemgetter(4))
            table_data = sorted(table_data, key=operator.itemgetter(1))
            table_data = sorted(table_data, key=operator.itemgetter(0))

            table_data.insert(0, table_header)
            return (section_head +
                    shared.web.html_formatting.list_of_lists_to_table(
                        table_data, escape=False))
        except zookeeper.exceptions.ConnectionLossException:
            return (section_head + '<br />Connecting to ZooKeeper')
        except:
            return (section_head + '<br />Internal Error: no data '
                                   'can be read right now')

    def _parse_row(self, data, zstat):
        """Format for display the data from a ZooKeeper node."""

        if data.get('hostname') and data.get('web_port'):
            data_link = '%s:%s' % (data['hostname'], data['web_port'],)
            link = '<a href="http://%s">%s</a>' % (data_link, data_link,)
        else:
            link = 'N/A'

        return [self.env_name, data.get('app_name', 'N/A'),
                data.get('hostname', 'N/A'), data.get('node_id', 'N/A'),
                data.get('node_state', 'active'),
                time.ctime(zstat.ctime / 1000.0), data.get('pid','N/A'), link]

    def _get_node_info(self):
        """Get static information about the running node.

        Provides descriptive info that will not change during the lifetime of
        the daemon.

        :return: A dictionary containing all information
        """
        obj = self.daemon_obj
        status = {}
        status['app_name'] = obj.app_name
        status['node_id'] = obj.node_id
        status['version'] = obj.VERSION
        status['hostname'] = obj.hostname
        status['pid'] = obj.pid
        status['user'] = obj.user
        status['group'] = obj.group
        status['app_started'] = obj.app_started
        status['daemon_start_time'] = obj._daemon_start_time
        status['app_start_time'] = obj._app_start_time

        web_plugin = self.daemon_obj._plugins.get('web')
        if web_plugin:
            status['web_port'] = web_plugin.port
        else:
            status['web_port'] = None

        rpc_plugin = self.daemon_obj._plugins.get('rpc')
        if rpc_plugin:
            status['rpc_port'] = rpc_plugin.port
        else:
            status['rpc_port'] = None
        return status

    @public_method
    def node_status_list(self):
        """Return node status list"""
        return [ x[0] for x in self._get_node_status_list() ]

    @rpc_method(name='node_status_list')
    def rpc_node_status_list(self):
        """Return node status list"""
        return [ x[0] for x in self._get_node_status_list() ]

    @zookeeper.helpers.retry()
    def _get_node_status_list(self):
        result_data = []
        apps = self._zookeeper.get_children(self._node_status_dir)
        for app in apps:
            nodes = self._zookeeper.get_children(
                '%s/%s' % (self._node_status_dir, app,))
            for node in nodes:
                try:
                    packed, zstat = self._zookeeper.get(
                        '%s/%s/%s' % (self._node_status_dir, app, node,))
                    unpacked = json.loads(packed)
                    result_data.append((unpacked, zstat))
                except zookeeper.exceptions.NoNodeException:
                    continue
        return result_data
