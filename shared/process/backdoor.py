"""Create a backdoor into a process.

Contains the server and handler to run a backdoor.

:Status: $Id: //prod/main/_is/shared/python/process/backdoor.py#9 $
:Authors: scottrwi, duncan
"""

import code
import errno
import logging
import os
import socket
import SocketServer
import sys

_log = logging.getLogger('backdoor')


class BackdoorHandler(code.InteractiveConsole):
    """Handle a backdoor session.

    Uses the Python builtin InteractiveConsole to handle console logic.
    This subclass simply adds reading input and sending output over a socket
    instead of stdout and stderr.
    """

    def __init__(self, socket, local_vars, welcome_message):
        """Handle a backdoor session.

        Called by BackdoorServer.  Runs the lifetime of the connection.
        The socket will be closed by server.
        """
        code.InteractiveConsole.__init__(self, locals=local_vars,
                                         filename='<input>')
        self._socket = socket
        self._output_fileobj = socket.makefile('w', 0)

        # Main loop: read input, process, send output.
        self.interact(banner=welcome_message)

    def raw_input(self, prompt):
        """Read from and write to socket and look for EOF.
        """
        self._socket.send(prompt)

        in_data = self._socket.recv(8192)
        if not in_data or '\004' in in_data:
            self._output_fileobj.close()
            raise EOFError

        return in_data.rstrip('\r\n')

    def runcode(self, code_obj):
        """Redirect stderr and stdout to the socket.
        """
        save = (sys.stdout, sys.stderr)
        try:
            sys.stdout = self._output_fileobj
            sys.stderr = self._output_fileobj
            code.InteractiveConsole.runcode(self, code_obj)
        finally:
            sys.stdout, sys.stderr = save

    def write(self, data):
        """Write to socket.
        """
        try:
            self._socket.sendall(data)
        except socket.error, e:
            # If client closes connection early , interact will still write a
            # newline character on the closed connection.  This raises a
            # Broken Pipe so ignore it.
            if e[0] == errno.EPIPE:
                pass
            else:
                raise


class BackdoorServer(SocketServer.ThreadingMixIn,
                     SocketServer.TCPServer):
    """A server for a backdoor.

    Uses TCPServer to allow access via TCP port.

    Usage:

    server = BackdoorServer((<hostname>, <port>), <local_vars_dict>,
                            <welcome_message>)

    # Usually want to start this in separate thread since it blocks.
    server.serve_forever()

    # Called from signal handler normally.
    server.shutdown()
    """

    # Ensure process can exit even if backdoor sessions are open.
    daemon_threads = True
    # Enable quick restarts without address already in use error.
    allow_reuse_address = True

    def __init__(self, server_address, local_vars=None, welcome_message=None):
        """Set instance variables.
        """
        SocketServer.TCPServer.__init__(self, server_address, BackdoorHandler)
        self.local_vars = local_vars
        self.welcome_message = welcome_message


    def finish_request(self, request, client_address):
        """Override this method to pass the locals value to our handler.
        """
        BackdoorHandler(request, self.local_vars, self.welcome_message)

    def handle_error(self, request, client_address):
        """Log the error instead of printing to stderr.
        """
        _log.exception('Error occured handling backdoor connection.')

