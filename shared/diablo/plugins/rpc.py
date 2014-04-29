"""RPC interface for Diablo.

Plugin for Diablo that starts FastRPC server to allow external clients
to call methods in a daemon.  Use FastRPC client to connect to the server.

:Authors: vkuznets
$Id: //prod/main/_is/shared/python/diablo/plugins/rpc.py#10 $
"""

import logging

import shared.util.decoratorutils
from shared.diablo.decorator_registrar import DecoratorRegistrar, bind
from shared.diablo.plugins import DiabloPlugin, generate_port
from shared.rpc.blocking_server import FastRPCServer


BASE_PORT = 0


class _AttributeProxy(object):
    """Instance of this class represents proxy to the attributes
    exposed by FastRPC server
    """

    def __init__(self, daemon):
        """Constructor.

        :Parameters:
            -`daemon`: Diablo daemon instance
        """

        self.__daemon = daemon
        self.__log = logging.getLogger(self.__daemon.__class__.__name__)
        self.__methods = {}

        self.__bind_rpc_methods(self.__daemon)
        for name in self.__daemon._plugins:
            if name == RPCPlugin.NAME:
                continue
            plugin_obj = self.__daemon._plugins[name]
            self.__bind_rpc_methods(plugin_obj)

        self.__log.debug('FastRPC server has these methods:\n%s',
                         self.__methods.keys())

    def __getattr__(self, name):

        if name in self.__methods:
            return self.__handle_rpc_call(self.__methods[name])

        error_msg = 'Does not support RPC method: %s' % (name,)
        self.__log.error(error_msg)
        raise AttributeError(error_msg)

    @property
    def exposed_methods(self):
        return list(self.__methods)

    def __handle_rpc_call(self, func):

        def decorator(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception, exc:
                self.__log.exception('Exception during the RPC call: %s', exc)
                raise

        return decorator

    def __bind_rpc_methods(self, obj):
        """Bind available rpc methods from obj."""
        for (method, method_name) in getattr(obj, 'RPC_METHODS', []):
            self.__methods[method_name] = bind(method, obj)


class RPCPlugin(DiabloPlugin):
    """Plugin for Diablo that starts FastRPC server to allow external
    clients to call methods in a Diablo daemon.

    To add methods to the interface use the "rpc_method" decorator.
    To enable this plugin put this line in your Diablo application __init__:

      self.register_plugin(shared.diablo.plugins.rpc.RPCPlugin)
    """

    NAME = 'rpc'

    def __init__(self, *args, **kwargs):
        """Initialize instance variables."""

        self.__log = logging.getLogger('diablo.plugins.rpc')

        super(RPCPlugin, self).__init__(*args, **kwargs)

        self.port = None
        self.server = None
        self._proxy = None

    def startup(self):
        """Start the rpc server and register the methods."""

        conf_port = self.conf.get('%s.rpc_server_port' %
                                        (self.conf_section,))
        if conf_port:
            self.port = conf_port + self.daemon_obj.node_id
        else:
            self.port = generate_port(BASE_PORT, self.daemon_obj.node_id,
                                      self.daemon_obj.app_name)

        self._proxy = _AttributeProxy(self.daemon_obj)
        self.server = FastRPCServer(self._proxy, ('', self.port))

        self.__log.info('FastRPC server started on port [%d].', self.port)
        self._threads.append(self.server)
        self.server.start()

    def get_status(self):
        """Return a dictionary of status information."""

        return {'port': self.port,
                'methods': self._proxy.exposed_methods}

    def shutdown(self):
        """Stop the RPC server."""

        self.__log.info('Shutting down FastRPC server.')
        self.server.kill()
        self.join_threads()


@shared.util.decoratorutils.allow_no_args_form
def rpc_method(name=None):
    """Set up an RPC method.

    Used as a decorator to indicate that this method provides an RPC method,
    and should be made accessible via the RPC server. The method's name
    (externally) is given by ``name``.

    If multiple methods in the same class specify the same name, the result is
    undefined.  If a subclass uses the same name as a parent class, the
    subclass's method will be used.
    """

    def decorator(func):
        """Register the function."""

        method_name = name
        if method_name is None:
            method_name = func.__name__

        DecoratorRegistrar.register('RPC_METHODS', func, method_name)
        return func

    return decorator
