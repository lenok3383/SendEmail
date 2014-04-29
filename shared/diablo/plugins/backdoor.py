"""Backdoor interface for Diablo.

Plugin for Diablo that starts backdoor server on a unix socket.  This allows
direct access into the daemon's namespace.

:Authors: scottrwi
$Id: //prod/main/_is/shared/python/diablo/plugins/backdoor.py#7 $
"""
import logging
import os.path
import sys

from shared.diablo.plugins import DiabloPlugin, generate_port
from shared.process.backdoor import BackdoorServer

BASE_PORT = 50


class BackdoorPlugin(DiabloPlugin):
    """"Plugin that runs a backdoor into the process.

    To enable this plugin put this line in your Diablo application __init__:

    self.register_plugin(shared.diablo.plugins.backdoor.BackdoorPlugin)

    To access the backdoor find the backdoor path via the get_status
    method or the web interface then on the same host the daemon is running:

    $> telnet localhost <port>

    This will give you access to a python interpreter shell inside the daemon
    process and namespace.
    """
    NAME = 'backdoor'

    def __init__(self, *args, **kwargs):
        """Initialize instance variables."""

        self.__log = logging.getLogger('diablo.plugins.backdoor')

        super(BackdoorPlugin, self).__init__(*args, **kwargs)

        self.port = None

    def startup(self):
        """Start the backdoor server."""

        conf_str = '%s.backdoor_port' % (self.conf_section,)
        conf_port = self.conf.get(conf_str)
        if conf_port:
            self.port = conf_port + self.daemon_obj.node_id
        else:
            self.port = generate_port(BASE_PORT, self.daemon_obj.node_id,
                                      self.daemon_obj.app_name)

        local_vars = sys.modules['__main__'].__dict__.copy()
        local_vars['daemon'] = self.daemon_obj

        welcome = []
        welcome.append(LOGO)
        welcome.append('Welcome to the backdoor of Diablo.\n')
        welcome.append('Python %s on %s\n\n' % (sys.version, sys.platform))
        welcome.append('=' * 80 + '\n')
        welcome.append('Access your application\'s object with "daemon".\n')
        welcome.append('>>> daemon.get_status()\n')
        welcome.append('=' * 80 + '\n')
        welcome = ''.join(welcome)

        # Bind only to localhost to avoid access from external host.
        self.server = BackdoorServer(('127.0.0.1', self.port), local_vars, welcome)

        self.__log.info('Backdoor started on port %d', self.port)

        self.start_in_thread('backdoor_server', self.server.serve_forever,
                             daemonize=False)

    def get_status(self):
        """Return a dictionary of status information."""

        return {'port': self.port}

    def shutdown(self):
        """Stop the backdoor server."""

        self.__log.info('Shutting down backdoor server.')
        self.server.shutdown()
        self.join_threads()
        self.server.server_close()


LOGO = r"""
        _______. _______   ______     ___      .______   .______     _______.
       /       ||   ____| /      |   /   \     |   _  \  |   _  \   /       |
      |   (----`|  |__   |  ,----'  /  ^  \    |  |_)  | |  |_)  | |   (----`
       \   \    |   __|  |  |      /  /_\  \   |   ___/  |   ___/   \   \
   .----)   |   |  |____ |  `----./  _____  \  |  |      |  |   .----)   |
   |_______/    |_______| \______/__/     \__\ | _|      | _|   |_______/

"""
