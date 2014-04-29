"""Base class for Diablo daemon.

Contains the DiabloBase class which must be inherited by any application
wanting to run on Diablo

For additional information look at the User's Guide:
http://eng.ironport.com/docs/is/shared/diablo_user_guide.rst

:Authors: scottrwi
$Id: //prod/main/_is/shared/python/diablo/base.py#8 $
"""

import grp
import logging
import os
import pwd
import socket
import threading
import time

from shared.diablo.decorator_registrar import DecoratorRegistrar, bind
from shared.diablo.plugins.web import web_method
from shared.diablo.plugins.rpc import rpc_method
from shared.util.decoratorutils import allow_no_args_form
from shared.web.html_formatting import dict_to_table


class DiabloBase(object):
    """Base class for Diablo framework

    It provides the basic functionality of initializing and starting
    the Diablo framework  and application threads and handling shutdown.

    All methods are always run on main thread except _run_app_loop(),
    stop_daemon(), and the web and rpc methods.
    """

    __metaclass__ = DecoratorRegistrar
    _APP_THREADS = ()

    def __init__(self, conf, conf_section, app_name, node_id):
        """Initialize instance variables

        All subclasses must call super at the beginning of __init__ like:

        super(<your class>, self).__init__(*args, **kwargs)

        :param conf: The dictionary-like object with all configuration
                     info.  Saved to self.conf.

        """
        self.__log = logging.getLogger('diablo.base')

        self.__log.info('Initalizing Diablo.')

        # Public attributes accessible by anyone.  Take extreme care if
        # modifying these.
        self.conf = conf
        self.conf_section = conf_section
        self.app_name = app_name
        self.node_id = node_id
        self.pid = os.getpid()
        self.hostname = socket.getfqdn()
        self.app_started = False
        self.user = pwd.getpwuid(os.geteuid())[0]

        gid = os.getegid()
        try:
            # Dev users are not assicated with a real group so this will
            # raise an exception.
            self.group = grp.getgrgid(gid)[0]
        except KeyError:
            self.group = str(gid)

        self._daemon_stopped_event = threading.Event()
        self._stop_reason = None
        self._app_threads = []
        self._daemon_start_time = None
        self._app_start_time = None

        self._plugins = {}
        self._shutdown_actions = []

    def register_plugin(self, cls, *args, **kwargs):
        """Register a plugin to be run by Diablo.

        The "cls" param is the class of the plugin you would like to run.
        """

        conf_str = '%s.enable_plugin_%s' % (self.conf_section, cls.NAME)
        enabled = self.conf.get(conf_str, True)
        if enabled:
            plug = cls(self, self.conf, self.conf_section, *args, **kwargs)
            self._plugins[cls.NAME] = plug
        else:
            self.__log.info('Plugin "%s" is disabled via config var %s',
                      cls.NAME, conf_str)

    def app_startup(self):
        """Start application.

        Called when starting application.  Application class should overrride
        this method with its own startup logic if it has any.  This method
        will always be empty so no need to call super.
        """
        pass

    def app_shutdown(self):
        """Shutdown application.

        Called when stopping application after all app threads have finished.

        This method is ALWAYS called if app_startup was called.

        Application class should overrride this method with its own shutdown
        logic if it has any.  This method will always be empty so no need to
        call super.
        """
        pass

    def get_app_status(self):
        """Return a dictionary of status information about the app.

        Applications may override this method to return a dictionary
        of status information.
        """
        return {}

    def should_continue(self):
        """Tells whether Diablo should keep running.

        :return: True until Diablo is told to stop.
        """
        return not self._daemon_stopped_event.is_set()

    def shallow_sleep(self, seconds):
        """Sleeps for a number of seconds, unless it is time to shutdown.

        Threads should call this to sleep for a given time interval, but wake
        if it needs to shut down. Returns False when it is time to shut down.
        """
        self._daemon_stopped_event.wait(seconds)

        return not self._daemon_stopped_event.is_set()

    def stop_daemon(self, reason=None):
        """Tell Diablo to stop running.

        Called when we want to start shutting down diablo.
        May be called from any thread or from application.
        """
        self._daemon_stopped_event.set()

        self._stop_reason = reason

    def run(self):
        """Run Diablo.

        Called by run_diablo.main.

        Blocks starting application until each plugin is ready for it.

        Blocks until application has finished and Diablo is fully shutdown.

        :raise Exception: Any uncaught exception encountered.
        """

        self.__log.info('Starting Diablo.')
        self._daemon_start_time = time.time()

        try:
            self.start_plugins()

            self.try_start_app()

            # Due to GIL weirdness you cannot just wait() because the main
            # thread is set to a "blocked" state and will never be scheduled
            # to process the signal.  If you give a max time argument to wait()
            # the main thread immediately wakes up to process the signal.
            # Therefore the max wait time given (60) can be very big.
            # This is the same reason we do not just _join_app_threads() here.
            while self.should_continue():
                self.shallow_sleep(60)

            if self._stop_reason:
                reason_str = ' because %s' % (self._stop_reason,)
            else:
                reason_str = ''

            self.__log.info('Diablo has been told to stop%s.', reason_str)

        finally:
            self.__log.info('Beginning shutdown process.')
            self._do_shutdown_actions()

    def start_plugins(self):
        """Call startup method of each plugin.

        Called at the start of run() method.
        """
        for name in sorted(self._plugins):
            obj = self._plugins[name]
            obj.startup()
            self._shutdown_actions.append(obj.shutdown)

            # Bind available public methods of the plugin to self
            for func, _ in getattr(obj, 'PUBLIC_METHODS', []):
                setattr(self, func.__name__, bind(func, obj))

    def should_start_app(self):
        """Call should_start_app method of each plugin.
        The application should not be started until each plugin's
        should_start_app() method returns True.

        Called after the startup() method of each plugin is called.
        """
        for name in self._plugins:
            obj = self._plugins[name]
            if not obj.should_start_app():
                return False
        return True

    def try_start_app(self):
        """Attempt to start application.

        Blocks starting the application with _start_app() call until
        should_start_app() returns True.
        """
        while self.should_continue():
            if not self.should_start_app():
                self.shallow_sleep(0.1)
            else:
                self._start_app()
                break

    def _start_app(self):
        """Start the application.

        Call the application startup method and start each app loop
        in its own thread.
        """
        self.__log.info('Starting application.')
        self._app_start_time = time.time()

        self.app_startup()
        self._shutdown_actions.append(self.app_shutdown)
        self.app_started = True

        # These actions are added before this loop because if any threads start
        # we want to ensure they get run.  There is no harm in them running
        # if no threads start.
        self._shutdown_actions.append(self._join_app_threads)
        self._shutdown_actions.append(self.stop_daemon)

        for func, (name, safe) in self._APP_THREADS:
            th = threading.Thread(target=self._run_app_loop, args=(func,),
                                  name=name)
            if not safe:
                # Don't worry about waiting for thread to stop.
                th.daemon = True

            self._app_threads.append(th)

            self.__log.debug('Starting application thread "%s".', name)
            th.start()

    def _run_app_loop(self, app_func):
        """Run an application loop.

        Started in its own thread.  Calls the application function
        and catches exceptions.

        :param app_func: The function to run.
        """
        thread_name = threading.current_thread().name
        self.__log.info('Running application thread "%s".', thread_name)
        try:
            app_func(self)
            self.__log.info('Application thread "%s" finished.', thread_name)
        except Exception:
            self.__log.exception('Error in application thread "%s"', thread_name)
        finally:
            # Always stop diablo when a app thread finishes or errors out.
            self.stop_daemon()

    def _join_app_threads(self):
        """Join on all application threads."""

        for th in self._app_threads:
            if th.daemon:
                # Don't worry about waiting for thread to stop.
                continue
            self.__log.info('Waiting for application thread "%s" to finish.',
                      th.name)
            th.join()

    def _do_shutdown_actions(self):
        """Execute all shutdown actions in reverse order.

        Each actions is guaranteed to be executed.  Actions added last will
        be run first.  Any exception is logged.
        """
        while self._shutdown_actions:
            func = self._shutdown_actions.pop()
            try:
                func()
            except Exception:
                self.__log.exception('Error calling shutdown action [%s]',
                               func.__name__)

    def get_info(self):
        """Get static information about the running daemon.

        Unlike `get_status()` this provides descriptive info that will not
        change during the lifetime of the daemon.

        :return: A dictionary containing all information
        """
        status = {}
        status['app_name'] = self.app_name
        status['node_id'] = self.node_id
        status['version'] = self.VERSION
        status['hostname'] = self.hostname
        status['pid'] = self.pid
        status['user'] = self.user
        status['group'] = self.group
        status['app_started'] = self.app_started

        status['daemon_start_time'] = self._daemon_start_time
        status['app_start_time'] = self._app_start_time
        return status

    def get_status(self):
        """Get status information about the running daemon.

        :return: A dictionary containing all status information
        """
        status = self.get_info()
        status['daemon_uptime'] = self.get_daemon_uptime()
        status['app_uptime'] = self.get_app_uptime()

        status['plugins'] = {}
        for name, obj in self._plugins.iteritems():
            status['plugins'][name] = obj.get_status()

        status['app'] = self.get_app_status()

        return status

    @web_method(name='status')
    def web_status(self, params):
        """Show status information."""

        return dict_to_table(self.get_status(), 'status_dict')

    @rpc_method(name='status')
    def rpc_status(self):
        """Return a dictionary of all status information."""

        return self.get_status()

    @web_method(name='restart')
    def web_restart(self, params):
        """Restart the server after short delay.

        Make sure the user really wants to do it by looking for the
        "confirm" parameter.
        """
        if 'submit' in params:
            self.stop_daemon()
            return '<h4>You have just told the daemon to restart.  Wait a few '\
                   'seconds and reload the <a href="/status">status</a> '\
                   'page.</h4>'

        msg = []
        msg.append('<h4>Are you sure you want to restart this '
                   'process?</h4>\n')
        msg.append('<form name="restart" action="/restart" '
                   'method="POST">')
        msg.append('<input type="submit" name="submit" '
                   'value="Yes I\'m Sure" /></form>')
        return ''.join(msg)

    @rpc_method(name='restart')
    def rpc_restart(self):
        """Restart the server after short delay.
        """
        self.stop_daemon()

    @web_method(name='log_level')
    def change_log_level(self, params):
        """Change log level of root logger.

        Give user links to change log level.
        """
        LEVELS = ('DEBUG', 'INFO', 'WARN', 'ERROR')
        root_log = logging.getLogger()
        curr_level = root_log.level

        if 'level' in params:
            new_level = params['level'][0].upper()
            if new_level in LEVELS:
                root_log.setLevel(getattr(logging, new_level))
                root_log.info('Log level changed to %s', new_level)
                return '<b>Root log level changed to "%s"</b>' % (new_level,)
            else:
                return '<b>Invalid log level "%s"</b>' % (new_level,)
        else:
            msg = []
            msg.append('<h3>Change log level</h3>\n')
            msg.append('<form name="change_log_level" '
                       'action="/log_level" method="POST">\n')
            for lev in LEVELS:
                checked = ''
                if curr_level == getattr(logging, lev):
                    checked = 'checked'
                msg.append('<input type="radio" name="level" value="%s" %s>'
                           ' %s</input><br />\n' % (lev, checked, lev))

            msg.append('<input type="submit" value="Submit" /><br />')
            msg.append('</form>\n')
            msg.append('<h4>Note: This will only change the level of the '
                       'root logger.  If you have configuration for more '
                       'specific loggers, they will not be affected.</h4>')
            return ''.join(msg)

    def get_daemon_uptime(self, formatted=False):
        """How long the daemon has been running."""

        return self._get_uptime(self._daemon_start_time, formatted)

    def get_app_uptime(self, formatted=False):
        """How long the application has been running."""

        return self._get_uptime(self._app_start_time, formatted)

    @staticmethod
    def _get_uptime(start, formatted):
        """Helper method to get uptime from a start time."""

        if start:
            uptime = time.time() - start
        else:
            uptime = 0.0

        return uptime


@allow_no_args_form
def app_thread(name=None, join_thread=True):
    """Designate this method an application thread

    Indicates a method should be started as a separate thread designed to run
    for the duration of the application's lifetime.

    The Diablo will restart itself if the thread dies or the method
    returns or raises an exception.

    The ``name`` parameter is used to set a thread name.  If omitted the
    function name is used.

    The ``join_thread`` parameter is set to False then the thread is in "daemon"
    mode meaning the thread does not need to end before Diablo exits.
    """

    def decorator(func):
        """Register the function."""
        thr_name = name
        if thr_name is None:
            thr_name = func.__name__

        DecoratorRegistrar.register('_APP_THREADS', func,
                                    (thr_name, join_thread))
        return func

    return decorator

