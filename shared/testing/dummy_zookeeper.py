"""Dummy ZooKeeper Node

Mimics a ZooKeeper connection to allow you to test or run where a ZooKeeper
cluster is not available.

:Author: parker
$Id: //prod/main/_is/shared/python/testing/dummy_zookeeper.py#1 $
"""

import time
from collections import defaultdict

# FUTURE TODO: remove the need for zookeeper totally
import zookeeper
import zookeeper.constants

# Helper function to keep track of our Dummy children nodes
def tree():
    return defaultdict(tree)

class DummyZooKeeperConnection():
    """Dummy ZooKeeper class that allows us to fake a connection to the ZooKeeper
    cluster.  Not all methods are covered but should be enough to handle everything
    that the Diablo plugins (zknode and zkft) need.  As with any dummy class the
    data returned can't really be trusted.

    This class does it's best to keep proper track of the children and the data
    assigned to nodes.  This is necessary because it will be used by the ZooKeeper
    locking class.
    """

    def __init__(self, connect_string, session_timeout=10000,
                 watcher=None, client_id=None):
        """Setup several variables to use later."""

        self._session_timeout = session_timeout

        self._ctime = time.time() * 1000
        self._mtime = time.time() * 1000
        self._owner = 'dummy_user'
        self._passwd = 'not_a_real_passwd'

        self._children = tree()
        self._node_data = {}

    def _add(self, t, path):
        """Used to add a path to the node tree."""

        if path.startswith('/'):
            path = path[1:]
        for key in path.split('/'):
            t = t[key]

    def _leaves(self, t, path):
        """Used to retrieve all of the leaves for a particular path."""

        leaf_nodes = []
        sub_tree = t # starting point
        if path.startswith('/'):
            path = path[1:]
        for node in path.split('/'):
            leaf_nodes = sub_tree[node].keys()
            sub_tree = sub_tree[node]
        return leaf_nodes

    def close(self):
        """Closing the connection, remove all nodes."""

        self._children = tree()
        self._node_data = {}

    def get_state(self):
        """Returns the current state of the connection, which for this class means
        that it's always connected.
        """

        return zookeeper.constants.CONNECTED_STATE

    def get_session_timeout(self):
        """Returns the currently set session timeout."""

        return self._session_timeout

    def create(self, path, data, acls, ephemeral=False, sequential=False):
        """Creates a node using the supplied data.

        NOTE: The acls, ephemeral and sequential arguments are ignored.
        """

        self._node_data[path] = data
        self._add(self._children, path)
        return path

    def get_children(self, path, watcher=None):
        """Gets the child leaves of the tree for path.

        NOTE: The watcher argument is ignored.
        """

        return self._leaves(self._children, path)

    def exists(self, path, watcher=None):
        """Checks to make sure that a path exists and returns a Stat object,
        otherwise it will return None.
        """

        if path in self._node_data:
            return zookeeper.Stat(None, # czxid
                                  None, # mzxid
                                  self._ctime, # ctime
                                  self._mtime, # mtime
                                  None, # version
                                  None, # cversion
                                  None, # aversion
                                  self._owner, # ephemeralOwner
                                  None, # dataLength
                                  len(self._leaves(self._children, path)), # numChildren
                                  None, # pzxid
                                  )
        else:
            return None

    def get_client_id(self):
        """Returns the client id (username, passwd) tuple."""

        return (self._owner, self._passwd)

    def set(self, path, data, version=-1):
        """Sets the data for the given path and returns a Stat object."""
        self._node_data[path] = data
        return zookeeper.Stat(None, # czxid
                              None, # mzxid
                              self._ctime, # ctime
                              self._mtime, # mtime
                              None, # version
                              None, # cversion
                              None, # aversion
                              self._owner, # ephemeralOwner
                              None, # dataLength
                              len(self._leaves(self._children, path)), # numChildren
                              None, # pzxid
                              )

    def delete(self, path, version=-1):
        """Delete the node with the given path.

        NOTE: The version argument is ignores.
        """
        del self._node_data[path]
        # FUTURE TODO: remove the path from self._children

    def get(self, path, watcher=None):
        """Return the data and the stat of the node of the given path.

        NOTE: The watcher argument is ignored.
        """

        data = self._node_data[path]
        stat = zookeeper.Stat(None, # czxid
                              None, # mzxid
                              self._ctime, # ctime
                              self._mtime, # mtime
                              None, # version
                              None, # cversion
                              None, # aversion
                              self._owner, # ephemeralOwner
                              None, # dataLength
                              len(self._leaves(self._children, path)), # numChildren
                              None, # pzxid
                              )
        return (data, stat)


