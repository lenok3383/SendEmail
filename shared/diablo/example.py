"""An example implementation of a Diablo application.

For additional information look at the User's Guide:
http://eng.ironport.com/docs/is/shared/diablo_user_guide.rst

:Authors: scottrwi
$Id: //prod/main/_is/shared/python/diablo/example.py#6 $
"""
import logging
import shared.diablo

# Looks for this file in $CONFROOT.
CONF_FILENAME = 'example.conf'

# The section name for diablo specific config in the conf file.
# Should be "diablo_<app_name>".
DIABLO_CONF_SECTION_NAME = 'diablo_example'

# The name of the application
APP_NAME = 'example'


class ExampleApplication(shared.diablo.DiabloBase):
    """A simple daemon to demo Diablo functionality.

    For additional information look at the User's Guide:
    http://eng.ironport.com/docs/is/shared/diablo_user_guide.rst
    """

    # Build version.  This should be filled in using the "@VERSION@" directive
    # filled in by the build script which should go in a separate file called
    # version.py.
    VERSION = '@VERSION@'

    def __init__(self, *args, **kwargs):
        """Whatever must be done when instance is created.  This function
        should run quickly.
        """
        # This "super" call must be first in this function.
        super(ExampleApplication, self).__init__(*args, **kwargs)

        self._log = logging.getLogger('example')

        # Registering plugins must be done in your __init__ method.
        self.register_plugin(shared.diablo.WebPlugin)
        self.register_plugin(shared.diablo.RPCPlugin)
        self.register_plugin(shared.diablo.BackdoorPlugin)

        self.counter = 0

    def app_startup(self):
        """Called at application startup.

        Anything that must be done when application is started but before
        the app threads begin running.  Implementing this method is optional.
        """
        self._log.info('Example application starting.')

    def app_shutdown(self):
        """Called at shutdown.

        Anything that must be done when application is being shut down but
        after the app threads have stopped running.  Implementing this method
        is optional.
        """
        self._log.info('Example application is being shut down.')

    def get_app_status(self):
        """Return a dictionary of status information."""
        return {'counter': self.counter}

    @shared.diablo.rpc_method
    def get_counter(self):
        """Return counter value."""
        return self.counter

    @shared.diablo.rpc_method
    def rpc_echo(self, echo):
        """Echo what is passed in back to the client."""
        return echo

    @shared.diablo.web_method(name='counter')
    def web_counter(self, params):
        """Show the counter value."""
        return '<p>Counter value: <b>%d</b></p>' % (self.counter,)

    @shared.diablo.app_thread
    def main_loop(self):
        """The main application loop.

        Any method with the app_thread decorator will be run in a separate
        thread.  An app thread method should check should_continue() as
        often as possible.  Once should_continue() is False, the method
        should return.  If the method raises an exception or returns
        before should_continue() is False, Diablo will automatically shut down.
        """
        while self.should_continue():
            self.counter += 1
            self.shallow_sleep(0.1)

    @shared.diablo.app_thread(join_thread=False, name='forever')
    def forever_loop(self):
        """This method loops forever.

        The "join_thread" parameter tells whether Diablo should wait for thread
        to finish before it shuts down.  The default is True.  If it's set
        to False, the thread will be set to "daemon" mode and Diablo will
        exit without the method finishing.
        """
        while self.should_continue():
            self._log.info('In forever_loop')
            self.shallow_sleep(5)

if __name__ == '__main__':
    # Call to main method to parse command line args and start the process.
    shared.diablo.main(ExampleApplication, CONF_FILENAME,
                       DIABLO_CONF_SECTION_NAME, APP_NAME)

