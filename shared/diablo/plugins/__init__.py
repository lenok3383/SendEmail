"""Contains Diablo plugin base class.

:Authors: scottrwi
$Id: //prod/main/_is/shared/python/diablo/plugins/__init__.py#6 $
"""

import hashlib
import logging
import os
import threading

import shared.util.decoratorutils
from shared.diablo.decorator_registrar import DecoratorRegistrar

class DiabloPlugin(object):
    """Base class for all Diablo plugins.

    Any plugin must inherit this class

    Plugin classes must define the class variable "NAME" or an AttributeError
    will be raised at init time.
    """
    __metaclass__ = DecoratorRegistrar

    def __init__(self, daemon_obj, conf, conf_section, *args, **kwargs):
        """Set instance variables.

        :param daemon_obj: The instance of diablo this plugin instance is
                           tied to.
        :param conf: A dictionary-like object containing all configuration.
        :param conf_section: The section of the config the plugin should look
                             for its config.

        Subclasses must include this line first:
        super(<your class>, self).__init__(*args, **kwargs)
        """
        self.__log = logging.getLogger('diablo.plugins')

        self.daemon_obj = daemon_obj
        self.conf = conf
        self.conf_section = conf_section

        self._threads = []

    def startup(self):
        """Called when Diablo starts up."""
        pass

    def should_start_app(self):
        """Should the Diablo application start up.
        For override in subclasses.
        """
        return True

    def shutdown(self):
        """Called when Diablo shuts down.

        Always called if startup method returned without exception.

        If you are using non-daemonized threads, call join_threads()
        in this method to ensure Diablo is able to exit.
        """
        pass

    def get_status(self):
        """Return the plugin's status information.

        Must return a dictionary of status information.
        """
        return {}

    def join_threads(self):
        """Wait for all threads to finish.

        Calls join() on each thread started using start_in_thread().

        Always call this method in shutdown() if you are using
        non-daemonized threads to ensure Diablo is able to exit.

        Only non-daemonized threads will be joined.
        """

        for th in self._threads:
            if not th.daemon:
                th.join()

    def start_in_thread(self, name, method, daemonize=False):
        """Run a method in a separate thread.

        This method creates thread and tells it to start and returns.

        The "method" given should run until shutdown() is called.  If the method
        returns early the entire daemon will exit.

        :param name: The name for the thread
        :param method: The method to run
        :param daemonize: Whether to daemonize the thread.  If False, the
                          daemon will not finish until the thread finishes.
                          In this case make sure and call join_threads()
                          in shutdown(). If True, the thread will not be
                          joined when join_threads() is called.
        """

        th = threading.Thread(target=self._run_method, args=(method,),
                              name=name)
        th.daemon = daemonize

        self._threads.append(th)

        self.__log.debug('Starting plugin thread "%s".', name)
        th.start()

    def _run_method(self, method):
        """Target method for thread to run.

        Handles logging and exception handling for running
        """

        thread_name = threading.current_thread().name
        self.__log.info('Running plugin thread "%s".', thread_name)
        try:
            method()
            self.__log.info('Plugin thread "%s" finished.', thread_name)
        except Exception, e:
            self.__log.exception('Error in plugin thread "%s"', thread_name)
        finally:
            # The daemon will always be stopped if the thread errors or the
            # method returns.
            self.daemon_obj.stop_daemon('plugin thread "%s" finished' %
                                            (thread_name,))


# Name of dev port offset environ variable.
DEV_PORT_OFFSET = 'DIABLO_DEV_PORT_OFFSET'
MAX_VALID_PORT = 65535


def generate_port(base_port, node_id, app_name):
    """Generate a port to bind to.

    :param base_port: 0 <= integer < 100
    :param node_id: integer >= 0
    :param app_name: The applciation's name

    To create the port the following are summed:

    1) Parameter base_port
    2) Node ID
    3) (Integer in range 11-99 based on md5 of app_name) * 100
    4) DIABLO_DEV_PORT_OFFSET environment variable

    :raise ValueError: if port is out of valid range.
    """
    port = base_port + node_id

    hasher = hashlib.md5()
    hasher.update(app_name)
    hashed = int(hasher.hexdigest()[:8], 16)
    hash_offset = ((hashed % 89) + 11) * 100
    port += hash_offset

    if DEV_PORT_OFFSET in os.environ:
        port += int(os.environ[DEV_PORT_OFFSET])

    if port > MAX_VALID_PORT:
        raise ValueError('Generated port %d is too big.  Try setting '
                         'environment variable %s to something smaller.',
                         port, DEV_PORT_OFFSET)

    return port

@shared.util.decoratorutils.allow_no_args_form
def public_method():
    """Define a public plugin method callable from the daemon object this
    plugin is registered on.

    If multiple methods in the same class have the same name, or if
    multiple plugins use the same name, the result is undefined.
    """

    def decorator(func):
        """Register the function."""
        DecoratorRegistrar.register('PUBLIC_METHODS', func, None)
        return func

    return decorator
