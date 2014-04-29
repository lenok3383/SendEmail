"""ZooKeeper FT for Diablo.

Plugin for Diablo that runs a ZooKeeper client to enable fault tolerance
with failover in a Diablo daemon.

:Author: taras, duncan
$Id: //prod/main/_is/shared/python/diablo/plugins/zkft.py#8 $
"""
import json
import logging
import threading
import time

import zookeeper
import zookeeper.helpers
import zookeeper.recipes.lock

import shared
from shared.diablo import web_method
from shared.diablo.plugins import public_method
from shared.diablo.plugins.zknode import ZooKeeperNodePlugin


class ZooKeeperFTPlugin(ZooKeeperNodePlugin):
    """Plugin for Diablo that runs a ZooKeeper client to enable fault
    tolerance with failover in a Diablo daemon.  The list of supported
    services should be passed in as the plugin constructor parameter.

    To enable this plugin put this line in your Diablo application __init__:

    self.register_plugin(shared.diablo.plugins.zkft.ZooKeeperFTPlugin,
                         service_names=['active'])

    Configuration:
        conf_section.zookeeper_env_product_name, default 'dev/<nicename>'
          Where, <nicename> is the last path segment from
          shared.conf.env.get_prod_root() using os.sep as a separator symbol.
          For /usr/local/ironport/whiskey the default will be 'dev/whiskey'.
          A warning message is shown during initialization of the plugin in
          case the default self-generated value is used.

        conf_section.zookeeper_conn_string, default '127.0.0.1:2181'

    Arguments for __init__:
        service_names - a mandatory list of services that should be run
    """

    NAME = 'zkft'

    def __init__(self, *args, **kwargs):
        """Initialize instance variables."""
        self.__log = logging.getLogger('diablo.plugins.zkft')

        if not 'service_names' in kwargs:
            raise ValueError('Missing required service_names param '
                             'for register_plugin')

        super(ZooKeeperFTPlugin, self).__init__(*args, **kwargs)

        self._service_names = kwargs['service_names']

        self.service_name = None
        self._service_locks = dict()
        self._service_data_rlock = threading.RLock()

    def get_services(self):
        """Returns a list of services that should be run.

        For each listed service, the plugin will attempt to acquire the
        service lock. If successful, the app will be started and
        `self.service_name` will be set accordingly.

        `self.should_start_app()` will return False in case service lock
        attempt.
        """
        return self._service_names

    def should_start_app(self):
        """Should the Diablo application start up.
        For override in subclasses.
        """
        return self.service_name is not None

    def startup(self):
        """Start the zkft plugin."""
        self.__log.info('Starting up FT plugin.')

        self._zk_register()

    def get_status(self):
        """Return a dictionary of status information."""
        status = super(ZooKeeperFTPlugin, self).get_status()
        if self.service_name is not None:
            status['service'] = self.service_name
        else:
            status['service'] = 'standing by'
        return status

    def shutdown(self):
        """Stop the ZooKeeper client."""
        self.__log.info('Shutting down FT plugin.')

        self._unlock_service()
        return super(ZooKeeperFTPlugin, self).shutdown()

    @public_method
    def safe_to_assert_master(self):
        """Returns True iff ZooKeeper is connected."""
        return (self._zookeeper.get_state() ==
                    zookeeper.constants.CONNECTED_STATE)

    @public_method
    def ensure_master(self):
        """Ensures caller is master of zookeeper node.

        If we are the master, return normally. If not, sleeps until
        we either become master and return normally or the session expires
        and we raise an exception.
        """
        while not self.safe_to_assert_master():
            if not self.daemon_obj.should_continue():
                raise DiabloShutdownException()
            self.daemon_obj.shallow_sleep(0.1)

    def _zk_connection_watcher(self, event):
        """Handle different ZooKeeper events.

        This completely replaces the parent's connection watcher,
        since we are much stricter about allowable events.
        """
        assert event.event_type == event.SESSION_EVENT
        state_string = zookeeper.Event.STATES[event.state]

        self.__log.debug('Connection watcher node %s, state %s.' %
                         (self.daemon_obj.node_id, state_string,))

        if event.state in (event.AUTH_FAILED_STATE,
                           event.EXPIRED_SESSION_STATE):
            # Once the client receives a fatal notification (AUTH_FAILED or
            # EXPIRED_SESSION) further access to ZooKeeper handle should be
            # avoided.  We should restart to avoid an undefined behavior.
            self.daemon_obj.stop_daemon('could not continue running '
                                        'in %s' % state_string)

        elif event.state == event.CONNECTING_STATE:
            # We are CONNECTING to the server.  From here, we may get
            # CONNECTED (if reconnected within session timeout) or get an
            # EXPIRED_SESSION (if reconnected after the timeout).
            self.__log.info('Lost connection to ZooKeeper (node %s)',
                            self.daemon_obj.node_id)

        elif event.state == event.CONNECTED_STATE:
            # We are successfully CONNECTED to the server. From here, the
            # handle can be in the CONNECTING or EXPIRED_SESSION state.
            self.__log.info('Connected to ZooKeeper (node %s).',
                            self.daemon_obj.node_id)

        else:
            self.daemon_obj.stop_daemon('caught myself in unexpected state %d'
                                        % (event.state,))

    # ZooKeeper service lock handling.
    #
    # The basic idea here is that we create a shared (ZooKeeper) lock for all
    # possible services we could run and then try to lock them.  Once we've got
    # one, we start that service and delete all claims to other locks.

    def _zk_register(self):
        super(ZooKeeperFTPlugin, self)._zk_register()

        # Figure out what to start.
        possible_services = self.get_services()
        for service in possible_services:
            service_lock = zookeeper.recipes.lock.Lock(
                self._zookeeper,
                '%s/lock/%s/%s' % (
                    self._namespace, self.daemon_obj.app_name, service),
                json.dumps([self.daemon_obj.hostname,
                            self.daemon_obj.node_id,
                            self.daemon_obj.pid]),
                self._acls, unique_id=self._unique_id)

            with self._service_data_rlock:
                self._service_locks[service] = service_lock

            try:
                got_it = service_lock.lock(self._make_lock_callback(service))
            except zookeeper.exceptions.ConnectionLossException:
                self.daemon_obj.stop_daemon('lost connection to ZooKeeper.')
                return

            if got_it:
                # No point continuing creating locks.  Lock callback will be
                # called regardless when the lock is acquired.
                break
        self._update_my_node()

    def _make_lock_callback(self, service_name):
        def callback():
            return self._got_lock(service_name)
        return callback

    def _got_lock(self, service):
        """Callback for ZooKeeper to notify us we have a lock."""
        with self._service_data_rlock:
            if not self.service_name:
                self.__log.info('Got service %s', service)
                self.service_name = service

                # We've got a lock.  Delete extra locks.
                for service in self._service_locks:
                    if service != self.service_name:
                        self._service_locks[service].cleanup()
        self._update_my_node()

    def _unlock_service(self):
        """Called as we exit to unlock the service by closing the
        client object.

        Client object is closed on every node.  ZooKeeper errors are caught
        and logged. By closing the client, ZooKeeper session becomes
        invalid, ephemeral nodes/locks/claims removed, watches on the nodes
        triggered.
        """
        try:
            self._zookeeper.close()
        except zookeeper.exceptions.ZooKeeperException, e:
            self.__log.warn('ZK Exception closing ZK client: %s', e)
        except Exception, e:
            self.__log.warn('Exception closing ZK client: %s', e)

    def _get_node_data(self):
        """Returns packed data to be stored on the node."""
        data = self._get_node_info()
        if self.service_name is not None:
            data['node_state'] = self.service_name
        else:
            data['node_state'] = 'standby'

        return json.dumps(data)

    @zookeeper.helpers.retry()
    def _update_my_node(self):
        """Update my node."""
        self.__log.debug('Updating node %s', self._my_node)

        packed_data = self._get_node_data()

        zookeeper.helpers.ensure_exists(self._zookeeper, self._node_status_dir)
        stat = self._zookeeper.exists(self._my_node)
        if stat:
            self._zookeeper.set(self._my_node, packed_data)
        else:
            self._zookeeper.create(self._my_node, packed_data,
                                   self._acls, ephemeral=True)

    # Web status: adds info about the service locks held by the nodes.

    @web_method(name='ft_status')
    def web_cluster_status(self, *args, **kwargs):
        """Provide node status and current lock owners."""
        start = self._get_web_node_status()
        rest = self._get_web_cluster_status()
        return start + rest

    @zookeeper.helpers.retry()
    def _get_web_cluster_status(self):
        """Report current owners of each service lock."""

        table_header = ['service', 'lock_node', 'ctime',
                        'mtime', 'ephemeral_owner',]
        table_data = []
        section_head = '<h1>Service info</h1>'

        try:
            apps = self._zookeeper.get_children('%s/lock' % (self._namespace,))
            for app in apps:
                zk_services = set(self._zookeeper.get_children(
                    '%s/lock/%s' % (self._namespace, app)))
                services = zk_services | set(self.get_services())
                for service in services:
                    try:
                        claims = self._zookeeper.get_children(
                            '%s/lock/%s/%s' % (self._namespace,
                                               app,
                                               service))
                    except zookeeper.exceptions.NoNodeException:
                        claims = []

                    if claims:
                        for claim in sorted(claims,
                                            key=lambda x: x.rsplit('-', 1)[1]):
                            zclaim = '%s/lock/%s/%s/%s' % (self._namespace,
                                                           app,
                                                           service, claim,)
                            try:
                                _, zstat = self._zookeeper.get(zclaim)
                            except zookeeper.exceptions.NoNodeException:
                                continue

                            lock_node = zclaim
                            ctime = time.ctime(zstat.ctime / 1000.0)
                            mtime = time.ctime(zstat.mtime / 1000.0)
                            ephemeral_owner = str(zstat.ephemeral_owner)

                            table_data.append(
                                [service, lock_node,
                                 ctime, mtime, ephemeral_owner])

            table_data.insert(0, table_header)
            return (section_head +
                    shared.web.html_formatting.list_of_lists_to_table(
                        table_data, escape=True))
        except zookeeper.exceptions.ConnectionLossException:
            return (section_head + '<br />Connecting to ZooKeeper')
        except:
            return (section_head + '<br />Internal Error: no data '
                                   'can be read right now')

class DiabloShutdownException(Exception):

    """Raised when ensure_master detects diablo is shutting down."""

    pass
