"""
Unittests for FastRPC package

:Authors: mskotyn, vkuznets
"""

import cPickle
import operator
import socket

from threading import Event


class FastRPCServerMock(object):
    """Mock for the FastRPCServer."""

    def __init__(self, root_object):
        self.kill_event = Event()
        self.root = root_object


class FastRPCClientMock(object):

    """Mock for the FastRPC client"""

    def __init__(self):
        self.called = []

    def request(self, *args):
        """Collect calls in the self.called list."""
        self.called.append(args)


class FastRPCServerSocketMock(object):
    """Mock for a connection."""

    def __init__(self, data=''):
        self.data = data
        self.send_data = []

    def settimeout(self, _val):
        """Set timeout mock method."""
        pass

    def recv(self, size):
        result = self.data[:size]
        self.data = self.data[size:]
        return result

    def close(self):
        pass

    def sendall(self, data):
        self.send_data.append(data)


class FastRPCClientSocketMock(object):

    """Mock for a socket for RPC client testing"""

    FAIL_ON_CONNECT = False

    def __init__(self, *args):
        response = cPickle.dumps((0, None, 6))
        self.__packet_len = '%08x' % (len(response),)
        self.__res_packet = ''.join([self.__packet_len, response])

    def __getattr__(self, name):
        return lambda *args, **kwargs: None

    def recv(self, size, *args):
        if size == 8:
            return self.__packet_len
        return self.__res_packet[8:]

    def connect(self, *args):
        if self.FAIL_ON_CONNECT:
            raise socket.error('timeout')


class TestRoot(object):

    """Class that knows how to add and multiply"""

    def add(self, *args):
        return reduce(operator.add, args)

    def mul(self, *args):
        return reduce(operator.mul, args)
