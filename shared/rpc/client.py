"""
FastRPC client implementation

Example of RPC client code usage:

    from shared.rpc import client

    # create FastRPCClient object which will communicate to the
    # server with address localhost:999
    clnt = client.FastRPCClient(('localhost', 9999))

    # get the proxy for the remote server so we can invoke
    # remote server methods as our own
    proxy = clnt.get_proxy()

    # call the remote methods
    print proxy.add(1,2,3,4,5)
    15

:Status: $Id: //prod/main/_is/shared/python/rpc/client.py#1 $
:Authors: ted, mskotyn, vkuznets
"""

import cPickle
import logging
import socket
import time
from threading import RLock

from shared.rpc import RPCError
from shared.rpc import RPCServerUnreachable


# Constants that represent client's state
DISCONNECTED = 0
CONNECTING = 1
CONNECTED = 2


class FastRPCProxy:

    """Proxy for a remote FastRPC server.

    Instance of this type is returned by FastRPCClient.get_proxy() call and
    represents a logical connection to the FastRPC server. Once user got proxy
    for the remote server, he can invoke remote server methods as his own
    """

    def __init__(self, conn, path=()):
        """Constructor.

        :Parameters:
            - `conn`: connection handler.
            - `path`: object path tuple. Default is empty tuple ().
        """

        self.__conn = conn
        self.__path = path

    def __getattr__(self, attr):
        """Get attribute.

        :Parameters:
            `attr`: attribute name.
        """

        if attr == '__call__':
            return self.__method_caller
        else:
            return FastRPCProxy(self.__conn, self.__path + (attr,))

    def __method_caller(self, *args):

        return self.__conn.request(self.__path, args)

    def __repr__(self):

        return '<remote-method-%s at %x>' % ('.'.join(self.__path), id(self))


class FastRPCClient(object):

    """FastRPC client.

    This implementation blockes when making an RPC request, resumed when the
    reply is returned. Has a number of features such as:

      - possibility to communicate to server listening on either
        network or UNIX socket
      - keep connection opened across the calls
      - retry logic on connection/RPC errors
      - optional callable to get the server address

    Please see constructor doc string for more info about available options
    """

    # Override these to control retry/timeout schedule.
    _retry_timeout = 2
    _forever_retry_timeout = 5

    def __init__(self, addr,
            forever_retry=False,
            retry_on_rpc_error=False,
            addr_func=None,
            timeout=None,
            num_retries=3,
            request_attempts=3):
        """
        :Parameters:
            - `addr`: Server address. (host, port) if server is listening on
                      network socket or string if server is listening on UNIX
                      socket
            - `forever_retry`: forever retry on connection and communication
                               errors. The default is False, only retry on
                               initial connection
            - `retry_on_rpc_error`: retry on RPCError. The default value is
                                    False. When it is turned on, the call will
                                    be blocked forever until the RPCError is
                                    gone. Please use it with extra caution
            - `addr_func`: if this function is provided, it will be revoked to
                           get the actual address to connect to. It is useful
                           to try through multiple server addresses. For example
                           this function can be used to provide the address of
                           the primary server in a fail-over setup.
            - `timeout`: a timeout for the socket calls. Defaults to None,
                         meaning no timeout. A value of 0 here would make the
                         socket perform nonblocking calls, so don't provide it!
            - `num_retries`: number of connection retries to make. When making a
                             request, if we are not connected, we will try to
                             connect for num_entries times and give up if we
                             can't (raising an RPCServerUnreachable error).
                             Defaults to 3.
            - `request_attempts`: number of request attempts to make. Defines
                                  how many times we will try to make a request
                                  when there are failures other than connection
                                  problems. Defaults to 3.
        """

        self.__log = logging.getLogger(self.__class__.__name__)
        self.__forever_retry = forever_retry
        self.__retry_on_rpc_error = retry_on_rpc_error
        self.addr = addr
        self.socket = None
        self.__addr_func = addr_func

        self._timeout = timeout
        self._num_retries = num_retries
        self._request_attempts = request_attempts

        self.conn = None
        self.state = DISCONNECTED

        self.__lock = RLock()

    def close(self):
        """
        Close the connection to the server.
        """

        if self.state != DISCONNECTED and self.socket:
            self.socket.close()
            self.state = DISCONNECTED

    def get_proxy(self):
        """Grab the proxy for the remote server."""

        return FastRPCProxy(self)

    def get_connected(self):
        """
        Try to connect to the server. Implements retry logic.

        :Return:
            - CONNECTED state on success, otherwise raise an exception

        :Exceptions:
            - RPCServerUnreachable: RPC server can not be reached
            - socket.error
        """
        if self.state == DISCONNECTED:
            self.state = CONNECTING
            for attempt_num in range(self._num_retries):
                self.__log.debug('FastRPC: connecting to server [attempt #%d]',
                                                                  attempt_num)
                try:
                    self.__connect()
                except socket.error as err:
                    if self.__forever_retry:
                        # Retry in different context.
                        self.state = DISCONNECTED
                        raise
                    self.__log.error('FastRPC: socket.error [%s]', err)
                    # If it's the last attempt, do not sleep.
                    if attempt_num != self._num_retries - 1:
                        time.sleep(self._retry_timeout)
                except:
                    self.state = DISCONNECTED
                    raise
                else:
                    self.__log.debug('FastRPC: connected to server')
                    return self.state

            # OK, we give up!
            # Fail any pending requests.
            self.state = DISCONNECTED
            raise RPCServerUnreachable('%s is unreachable' % (self.addr,))

        return self.state

    def request(self, path, params):
        """
        Send request to the server and get an answer

        :Parameters:
            - `path`: RPC call path
            - `params`: paramters

        :Return:
            - the result returned by server

        :Exceptions:
            - RPCServerUnreachable: coud not connect to the server
            - RPCError: various reasons, please check exception string for
                        details
            - socket.error: various reasons, please check exception string for
                            details
        """

        attempt = 0
        while 1:
            try:
                attempt += 1
                return self.__request(path, params)
            except (socket.error, OSError, EOFError) as err:
                if not self.__forever_retry and \
                        attempt >= self._request_attempts:
                    raise
                else:
                    self.__log.error('FastRPC: [%s] network error: %s',
                                     self.addr, err)
                    self.__log.error('FastRPC: retry network error - '\
                                     'attempts #%s', attempt)
                    time.sleep(self._forever_retry_timeout)
            except RPCError as err:
                if not self.__retry_on_rpc_error:
                    raise
                if not self.__forever_retry and \
                        attempt >= self._request_attempts:
                    raise
                else:
                    self.__log.error('FastRPC: [%s] RPCError: %s', self.addr,
                                                                   err)
                    self.__log.error('FastRPC: retry RPCError - attempts #%s',
                                     attempt)
                    time.sleep(self._forever_retry_timeout)

    def __recv_exact(self, size):
        """Recieve exactly the given number of bytes
        from the socket

        :Parameters:
            -`size`: the amount of bytes to receive

        :Return:
            - data string

        :Exceptions:
            - EOFError
        """

        size_left = size
        packet = ''
        while size_left > 0:
            temp_packet = self.socket.recv(size_left)
            if temp_packet == '':
                raise EOFError
            packet += temp_packet
            size_left -= len(temp_packet)
        return packet

    def __connect(self):
        """
        Create a socket(either network or UNIX) and try
        to connect to server. Does not implement any retry and
        error handling logic
        """

        if self.__addr_func:
            self.addr = self.__addr_func()
        if isinstance(self.addr, basestring):
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(self._timeout)
        self.socket.connect(self.addr)
        self.state = CONNECTED

    def __request(self, path, params):
        """Perform request to the server
        """

        self.__lock.acquire()
        try:
            if self.state != CONNECTED:
                if self.state == DISCONNECTED:
                    self.get_connected()

                if self.state != CONNECTED:
                    return None

            packet = cPickle.dumps((0, path, params))
            data = []
            data.append('%08x' % (len(packet),))
            data.append(packet)
            try:
                self.socket.sendall(''.join(data))
                reply = self.socket.recv(8, socket.MSG_WAITALL)
                if reply == '':
                    raise EOFError
                size = int(reply, 16)
                packet = self.__recv_exact(size)
                error, result = cPickle.loads(packet)[1:3]

            except (socket.error, OSError, EOFError):
                self.state = DISCONNECTED
                self.socket.close()
                raise

            if error:
                raise RPCError(error)
            else:
                return result
        finally:
            self.__lock.release()
