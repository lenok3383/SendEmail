"""counter_stroage module for FastRPC type storage. This is being separated so users of this
counter_storage module are not required to have MySQLdb libraries installed.

:Status: $Id $
:Authors: bwhitela
"""

import shared.monitor.counter
import shared.monitor.storage_types
import shared.rpc
import shared.rpc.client


class FastRPCCounterStorage(shared.monitor.counter.BaseCounterStorage):

    """Counter storage class which sends counter values to a remote FastRPC
    server."""

    def __init__(self, remote_addr, remote_port, remote_method='post_counter',
                 max_buffer_size=1024):
        """Constructor.

        :param remote_addr: hostname or IP for FastRPC server
        :param remote_port: port number for FastRPC server
        :param remote_method: RPC method to call
        :param max_buffer_size: buffer size for socket communication
        """
        self.remote_host = remote_addr
        self.remote_port = remote_port
        self.remote_method = remote_method
        self.buffer = []
        self.max_buffer_size = max_buffer_size
        client = shared.rpc.client.FastRPCClient((self.remote_host, self.remote_port))
        self.proxy = client.get_proxy()

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
            except shared.rpc.RPCError as e:
                raise shared.monitor.storage_types.StorageError(str(e))
            except shared.rpc.RPCServerUnreachable as e:
                raise shared.monitor.storage_types.StorageError('Cannot reach RPC server: %s' % (e,))
            if not success:
                raise shared.monitor.storage_types.StorageError('Counter storage failed')
            self.buffer.pop(0)
            if not self.buffer:
                break
