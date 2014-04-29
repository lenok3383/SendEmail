"""
This file contains RPC server implementation using blocking socket
calls. A server thread is created for each session between a client
and server. Calls on one session are synchronized.

:Status: $Id: //prod/main/_is/shared/python/rpc/blocking_server.py#1 $
:Authors: ted, mskotyn, vkuznets
"""

import cPickle
import errno
import logging
import socket
import traceback
from threading import Event
from threading import Thread


# The list of errnos expected in case if signal arrives.
EXPECTED_ERRNOS = (errno.EAGAIN, errno.EINTR)

class _FastRPCServerChannel(Thread):

    """RPC Server channel. Starts as a separate thread for each incoming
    client connection. Waits for RPC packets. Once the request has been
    received, it try to perform_request(). The return value of the invoked
    function will be sent back.

    This version of perform_request will block while executing
    a request.  If you want asynchronous behavior you'll need to
    execute and push the reply from another thread.
    """

    def __init__(self, server, conn, addr):
        """
        Constructor.

        :Parameters:
            - `server`: FastRPCServer instance
            - `conn`: socket object usable to send and receive data
            - `addr`: address bound to the socket on the client side
        """
        self.__channel_name = 'channel %s' % (addr,)
        self.__log = logging.getLogger(self.__channel_name)
        self.kill_event = server.kill_event
        self.root = server.root
        self.socket = conn
        self.socket.settimeout(1)

        Thread.__init__(self, name = self.__channel_name)

    def __recv_exact(self, size):
        """Wait until received exact amount of bytes specified by size.

        :Parameters:
            `size`: number of butes to be received.

        :Return:
            - data string
        """

        size_left = size
        data = []
        while not self.kill_event.isSet() and size_left > 0:
            try:
                temp_packet = self.socket.recv(size_left)
                if temp_packet == '':
                    raise EOFError
                data.append(temp_packet)
                size_left -= len(temp_packet)
            except socket.timeout:
                # Ignore timeout error.
                pass
            except socket.error as err:
                if err[0] not in EXPECTED_ERRNOS:
                    self.__log.warn('Socket Error on recv(): ', exc_info=1)

        if size_left == 0:
            return ''.join(data)
        else:
            raise EOFError

    def run(self):
        """Channel thread main loop."""

        try:
            while not self.kill_event.isSet():
                # Waiting for request.
                size = int(self.__recv_exact(8), 16)
                request = self.__recv_exact(size)
                ident, path, params = cPickle.loads(request)
                result = self.perform_request(ident, path, params)

                # Send result back.
                packet = cPickle.dumps(result)
                data = []
                data.append('%08x' % (len(packet)))
                data.append(packet)
                self.socket.sendall(''.join(data))
        except EOFError:
            pass
        except Exception:
            self.__log.exception('%s errored out', self.__channel_name)

        self.handle_close()

    def handle_close(self):
        """Clean up when channel closed."""

        self.socket.close()

    def perform_request(self, ident, path, params):
        """Actually invoke the method defined by path.

        :Parameters:
            `ident`: request id for the channel.
            `path`: rpc call path.
            `params`: params for the call.

        :Return: tuple (ident, error if any, result from the call).
        """

        obj = self.root
        err = None
        try:
            for pth in path:
                obj = getattr(obj, pth)
            result = obj(*params)
        except Exception:
            err = traceback.format_exc()
            result = None
        return (ident, err, result)


class FastRPCServer(Thread):

    """Simple rpc server.

    It is implemented using blocking socket calls. Creates separate
    server thread(_FastRPCServerChannel instance) for each session between
    a client and server.
    """

    def __init__(self, root, addr):
        """Constructor.

        :Parameters:
            `root`: the instance that handles all incoming RPC calls.
            `addr`: server address to listen on. It may be either (host, port)
                    tuple for network socket or string for UNIX domain socket
        """

        self.__log = logging.getLogger(self.__class__.__name__)
        self.kill_event = Event()
        self.root = root
        self.addr = addr

        if isinstance(addr, basestring):
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        Thread.__init__(self, name='RPC Server @ %s' % (addr,))

    def run(self):
        """RPC server main loop.

        Binds the socket to address. While not killed, accepts all incoming
        connections, starts _FastRPCServerChannel thread for each new connection
        """

        self.socket.settimeout(5)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.addr)
        self.socket.listen(10)
        while not self.kill_event.isSet():
            try:
                conn, addr = self.socket.accept()
            except socket.timeout:
                # loop back and listen again
                continue
            except socket.error, why:
                if why[0] not in EXPECTED_ERRNOS:
                    self.__log.warn('Socket Error on accept(): ', exc_info=1)
                continue

            channel = _FastRPCServerChannel(self, conn, addr)
            channel.start()
        self.__log.debug('RPC server main loop ended')

    def kill(self):
        """Signal all threads to quit and stop the server"""

        self.kill_event.set()
