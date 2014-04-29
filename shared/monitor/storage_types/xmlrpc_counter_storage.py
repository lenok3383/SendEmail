"""counter_stroage module for XMLRPC type storage. This is being separated so users of this
counter_storage module are not required to have MySQLdb libraries installed.

:Status: $Id $
:Authors: bwhitela
"""

import socket
import xmlrpclib

import shared.monitor.counter
import shared.monitor.storage_types


class XMLRPCCounterStorage(shared.monitor.counter.BaseCounterStorage):

    """Counter storage class which sends counter values to a remote XMLRPC
    server."""

    def __init__(self, remote_addr, remote_method='post_counter',
                 max_buffer_size=1024):
        """Constructor.

        :param remote_addr: hostname or IP for XMLRPC server
        :param remote_method: RPC method to call
        :param max_buffer_size: buffer size for socket communication
        """
        self.remote_host = remote_addr
        self.remote_method = remote_method
        self.buffer = []
        self.max_buffer_size = max_buffer_size
        self.proxy = xmlrpclib.ServerProxy(self.remote_host)

    def store(self, key, value, counter_name):
        """Send the counter value to the remote host.

        :param key: counter key
        :param value: counter value (integer)
        :param counter_name: counter name (may differ from key)

        :raises: shared.monitor.storage_types.StorageError
        """

        self.buffer.append((key, value, counter_name))
        while len(self.buffer) > self.max_buffer_size:
            self.buffer.pop(0)
        while True:
            try:
                success = getattr(self.proxy, self.remote_method)(*self.buffer[0])
            except socket.error as e:
                raise shared.monitor.storage_types.StorageError(str(e))
            if not success:
                raise shared.monitor.storage_types.StorageError('Counter storage failed')
            self.buffer.pop(0)
            if not self.buffer:
                break
