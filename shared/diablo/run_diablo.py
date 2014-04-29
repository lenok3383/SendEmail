"""Initalize and run Diablo.

Contains a main method that starts Diablo from the command line.  It allows
the user to start Diablo as a single process in the foreground or as a parent
process with multple child processes.  It can also daemonize the parent
process.

:Authors: duncan, ereuveni, scottrwi
$Id: //prod/main/_is/shared/python/diablo/run_diablo.py#11 $
"""

import logging.config
import logging.handlers
import optparse
import os
import pwd
import signal
import sys
import threading

import shared.conf.env
import shared.process.daemon
import shared.process.processutils
import shared.logging.formatters
import shared.logging.loggers

DIABLO_RETURN_SUCCESS = 0
DIABLO_RETURN_ERROR = 2

# Should probably come from logging module in shared.
DEFAULT_LOG_FORMAT_STRING = '%(asctime)s [pid:%(process)d %(name)s ' \
                            '%(filename)s:(%(lineno)d)] %(levelname)s: ' \
                            '%(message)s'


def main(daemon_class, cfg_filename, cfg_section_name, app_name):
    """Parses the command line, creates config object run the diablo
    application.

    Based on the command line, the application is either launched in the
    foreground, or the process will daemonize and run it in the background.

    :Parameters:
        - `daemon_class`: Main daemon class.
        - `cfg_filename`: Name of config file to read (with or without
          ``.conf`` extension)
        - `cfg_section_name`: Name of section in config to read.
        - `app_name`: Name of the application.
    """

    options = get_options_from_command_line(daemon_class)

    # shared.conf.get_config adds the .conf suffix, strip it here:
    if cfg_filename.endswith('.conf'):
        assert '/' not in cfg_filename, 'cfg_filename should be the name ' \
            '(not a path) of a file in $CONFROOT.'
        cfg_filename = cfg_filename[:-5]

    conf = shared.conf.get_config(cfg_filename)

    # Set the app name for the shared.logging items, in case they are used.
    shared.logging.formatters.set_app_name(app_name)

    if options.daemon or options.parent:
        parent(conf, cfg_section_name, app_name, options)
    else:
        child(daemon_class, conf, cfg_section_name, app_name, options)


def child(daemon_class, conf, cfg_section_name, app_name, options):

    """Run the application.

    This is used when the application is set to run in the foreground. When the
    application is run as a daemon, this mode is used by the child processes.
    """

    def kill_signal_handler(signum, frame):
        """Tell diablo to stop
        """
        diablo.stop_daemon('received signal %d' % (signum,))

    config_child_logging(options, app_name)

    _log = logging.getLogger('diablo.run.child')

    diablo = daemon_class(conf, cfg_section_name, app_name, options.node_id)

    _install_stop_signal_handlers(kill_signal_handler)

    try:
        diablo.run()
        ret_code = DIABLO_RETURN_SUCCESS
    except:
        _log.exception('Encountered error, exiting.')
        ret_code = DIABLO_RETURN_ERROR

    _log.info('Diablo done.')
    sys.exit(ret_code)


def _install_stop_signal_handlers(handler):
    """Registers handler to run when shutdown signals are recieved.

    Should only be called from main thread.
    """
    for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM,
                signal.SIGQUIT, signal.SIGABRT):
        signal.signal(sig, handler)


def parent(conf, conf_section, app_name, options):

    """Start application in parent mode.

    This will spawn num_nodes children and restart them if they die. Can be run
    in the foreground (-p, --parent) or background (-d, --daemon).
    """

    config_parent_logging(options, app_name)

    max_kill_delay = int(conf.get('%s.max_kill_delay' % (conf_section,), 60))
    num_nodes = int(conf.get('%s.num_nodes' % (conf_section,), 1))

    if num_nodes < 1:
        num_nodes = 1

    # Prepare background process arguments.
    python_executable = shared.process.processutils.get_python_executable()
    args = [python_executable, os.path.realpath(sys.argv[0])]

    # Here we pass the options on to the sub process.
    args.extend(get_child_log_options(options))

    # NodeMonitor adds the '--node-id 0' args.  If we're in daemon mode, we
    # need to pipe stdout/stderr to the log file.
    node_monitor = NodeMonitor(args, num_nodes, max_kill_delay,
                               pipe_fds=bool(options.daemon))

    if options.daemon:
        piddir = conf.get('%s.pid_dir' % (conf_section,), '/tmp/')
        pidfile = os.path.join(piddir, '%s.pid' % (app_name,))
        user = conf.get('%s.user' % (conf_section,), None)
        group = conf.get('%s.group' % (conf_section,), None)
        allow_root = conf.get('%s.allow_root_user' % (conf_section,), False)

        # Allow developer to run without having to sudo.  If config is the
        # same as current user, do not change user/group when daemonizing.
        # Dev users have no real group associated with them so only look at
        # user.

        curr_user = pwd.getpwuid(os.geteuid())[0]
        if curr_user == user:
            user = None
            group = None

        # Don't let us run as root if we are not dropping privileges.
        if (os.geteuid() == 0 and not allow_root) \
           and (user is None or group is None):
            raise Exception('Must configure daemon user and group if '
                            'running as root!')

        # Similarly, provide a useful error if we're running as non-root and
        # expect to change user/group.

        if os.geteuid() != 0 and (user is not None or group is not None):
            raise Exception('Must run as root if changing user or group!')

        shared.process.daemon.daemonize(node_monitor, (), pidfile, user, group)

    else:
        node_monitor()


class NodeMonitor(object):

    """Starts child processes (nodes) and monitors them.

    This class takes care of starting up one or more subprocesses and killing
    them when the process is killed.  The class is instantiated with the
    arguments for the subprocess, the number of nodes to instantiate, and the
    maximum amount of time to wait for a subprocess to die.

    When each subprocess is started, it is given the '--node-id' argument with
    an integer, in addition to all the other arguments provided.
    """

    # Event object created in __init__.
    _killed_event = None
    _signum = None

    def __init__(self, args, num_nodes, max_kill_delay, pipe_fds):

        _install_stop_signal_handlers(NodeMonitor.signal_handler)
        NodeMonitor._killed_event = threading.Event()

        self._log = logging.getLogger('diablo.run.parent.NodeMonitor')
        self._args = args
        self._num_nodes = num_nodes
        self._max_kill_delay = max_kill_delay
        self._pipe_fds = pipe_fds

    @staticmethod
    def signal_handler(signum, frame):
        """Signal handler for receiving signals."""
        NodeMonitor._signum = signum
        NodeMonitor._killed_event.set()

    def stop_process(self, process):
        """Stops a subprocess."""
        try:
            process.stop(sig=NodeMonitor._signum,
                         max_kill_delay=self._max_kill_delay)
        except:
            self._log.exception('Exceptions when stopping process %s',
                                process.name)

    def __call__(self):
        """Start running one or more background processes."""

        while not NodeMonitor._killed_event.is_set():
            processes = list()

            for i in range(self._num_nodes):
                node_args = self._args + ['--node-id', str(i)]
                processes.append(shared.process.processutils.MonitoredProcess(
                    name='diablo.subprocess_%d' % (i,),
                    args=node_args, pipe_stdout=self._pipe_fds,
                    pipe_stderr=self._pipe_fds))

            for process in processes:
                process.start()

            while not NodeMonitor._killed_event.is_set():
                NodeMonitor._killed_event.wait(60)

            self._log.info('Parent process received signal %s.', self._signum)

            # Stop processes in parallel to reduce latency.
            stop_threads = list()
            for process in processes:
                stop_thr = threading.Thread(target=self.stop_process,
                                            args=(process,))
                self._log.debug('Stopping process %s' % (process.name,))
                stop_thr.start()
                stop_threads.append(stop_thr)

            for stop_thr in stop_threads:
                stop_thr.join()

            if NodeMonitor._signum == signal.SIGHUP:
                # SIGHUP only reset the server.
                self._log.info('SIGHUP, ready to restart')
                NodeMonitor._killed_event.clear()
                NodeMonitor._signum = 0

        self._log.info('Parent process done.')


def config_child_logging(options, app_name):
    """Configures the logging system for children processes."""

    if options.log_conf:
        _config_log_conf(options.log_conf)

    else:
        if options.syslog:
            _config_log_syslog(app_name)
        else:
            _config_log_stderr()

        _config_log_level(options.verbosity)


def config_parent_logging(options, app_name):
    """Configures the logging system for the parent/daemon processes.

    In daemon mode, ONLY logs to syslog, to work around permissions issues
    in python 2.4's file logging. In parent mode ONLY logs to stderr, for
    same reasons.

    Returns the option strings required to pass on to the child process.
    """

    # Default for daemon mode is to log this process to syslog.
    if options.daemon:
        _config_log_syslog(app_name)

    # Default for parent mode is to log this process to stderr.
    else:
        _config_log_stderr()

    _config_log_level(options.verbosity)


def get_child_log_options(options):
    """Returns the options string required to pass on to the child process."""

    # Only to be passed down if log-conf not used.
    pass_verbosity_to_child = True

    # --logconf
    if options.log_conf:
        log_options = ['--logconf', options.log_conf]
        pass_verbosity_to_child = False

    else:
        # --syslog
        if options.syslog:
            log_options = ['-s']

        # defaults
        else:
            # For --daemon, default is to log to a log.conf if one exists,
            # otherwise syslog.
            if options.daemon:
                default_log_conf = os.path.join(shared.conf.env.get_conf_root(),
                                                'log.conf')
                if os.path.exists(default_log_conf):
                    log_options = ['--logconf', default_log_conf]
                    pass_verbosity_to_child = False
                else:
                    log_options = ['-s']

            # We're in the foreground (parent mode).
            # Default here is to log to stderr.
            else:
                log_options = []

    # Optionally pass along verbosity to children.
    if pass_verbosity_to_child:
        if options.verbosity < 1:
            log_options.append('-q')
        elif options.verbosity > 1:
            log_options.append('-v')

    return log_options


def _config_log_stderr():
    """Configure logging to use stderr."""

    logger = logging.getLogger()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT_STRING))

    logger.addHandler(handler)


def _config_log_syslog(app_name):
    """Configure logging to go to syslog."""

    logger = logging.getLogger()
    handler = shared.logging.loggers.SafeSysLogHandler(
                  '/dev/log',
                  logging.handlers.SysLogHandler.LOG_LOCAL6)
    formatter = logging.Formatter('%s: %s' % (app_name,
                                              DEFAULT_LOG_FORMAT_STRING))
    handler.setFormatter(formatter)

    logger.addHandler(handler)


def _config_log_conf(log_conf):
    """Configure logging using a log.conf."""

    confroot_filename = os.path.join(shared.conf.env.get_conf_root(),
                                     log_conf)
    if os.access(confroot_filename, os.R_OK):
        log_conf = confroot_filename

    if not os.access(log_conf, os.R_OK):
        raise IOError('Could not read log configuration file.\n'
                      'Tried looking in $CONFROOT (%s).\n'
                      'Tried as absolute path (%s).' %
                      (confroot_filename, log_conf))

    try:
        logging.config.fileConfig(log_conf, disable_existing_loggers=0)
    except KeyError, e:

        # fileConfig exception handling is quite poor.  We're compensating here
        # by catching the most common one (a KeyError on a log pointing to a
        # file that it cannot open) and providing a suggestion for how to fix.

        raise Exception('Cannot load log config from file: %s. '
                        'Check permissions on all files pointed to in the '
                        'log config.' % (e,))


def _config_log_level(verbosity):
    """Set log level based on verbosity."""

    llevel = logging.INFO
    # -q provided.
    if verbosity < 1:
        llevel = logging.WARN
    # -v provided.
    elif verbosity > 1:
        llevel = logging.DEBUG

    logger = logging.getLogger()
    logger.setLevel(llevel)


def get_options_from_command_line(daemon_class, cmd_line_args=None):
    """Create the option parser and parse the args

    :param daemon_class: The class that will be instantiated.
    :param cmd_line_args: The list of arguments to parse.
                          Defaults to sys.argv.

    :return: The parsed options object
    """
    version = '%%prog %s' % (daemon_class.VERSION,)
    option_parser = optparse.OptionParser(version=version)

    option_parser.add_option(
        '-d', '--daemon', action='store_true',
        dest='daemon', help='run process as a daemon')
    option_parser.add_option(
        '-p', '--parent-mode', action='store_true',
        dest='parent',
        help='run in "parent mode", spawning and monitoring <num_nodes> '
        'children')
    option_parser.add_option(
        '--node-id', action='store', type='int',
        dest='node_id', default=0, help='node id for this given '
        'child instance.  User should not specify this option.')
    option_parser.add_option(
        '-v', '--verbose', action='store_const', const=2, default=1,
        dest='verbosity', help='set debug level verbosity '
        '(default "info" level; ignored if log.conf used)')
    option_parser.add_option(
        '-q', '--quiet', action='store_const', const=0,
        dest='verbosity', help='set warning level verbosity '
        '(default "info" level; ignored if log.conf used)')
    option_parser.add_option(
        '-l', '--logconf', action='store', type='string',
        dest='log_conf', default=None,
        help='logging configuration file (default: $CONFROOT/log.conf in '
        '--daemon mode)')
    option_parser.add_option(
        '-s', '--syslog', action='store_true', dest='syslog', default=False,
        help='log to syslog (default in --daemon mode if no log.conf found)')

    if cmd_line_args is None:
        cmd_line_args = sys.argv[1:]

    options, _ = option_parser.parse_args(cmd_line_args)

    if options.log_conf and options.syslog:
        # error() exits program.
        option_parser.error('Can not select both log options: --logconf and '
                            '--syslog')

    return options
