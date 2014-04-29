"""Abstracts the notion of daemonizing a process in python.  To use:

from shared.process import daemon

def mymain(port):
    # bind to port and wait for requests

if __name__ == '__main__':
    (options, args) = daemon.get_option_parser(port=10000).parse_args()
    daemon.daemonize(mymain, (options.port,), options.pidfile,
                     options.user, options.group)


During the porting to Python 2.6 we've made the following changes
which break compatibility with existing IronPort products:

    - 'wide_pid' option removed from get_option_parser result,
    - 'wide_pid' argument removed from daemonize.

:Status: $Id: //prod/main/_is/shared/python/process/daemon.py#11 $
:Authors: jwescott, rbodnarc
"""

import grp
import logging
import optparse
import os
import pwd
import sys

from shared.process import setuid


DEFAULT_PID_PATHNAME = '/var/run/%s.pid' % (os.path.basename(
                                            sys.argv[0]),)
DEFAULT_USER = None
DEFAULT_GROUP = None

def get_option_parser(port=None, default_pid_pathname=None):
    """Create an OptionParser with a prepopulated set of arguments.

    Those arguments are often used as command line arguments for daemons.
    Note: option 'wide_pid' has gone.  It was related to a bug in rc.d in
    FreeBSD 6.3, which is fixed in FreeBSD 7.3 already.

    :param port: Default value for a port option.
    :param default_pid_pathname: Default value for a pidfile option.
    :return: OptionParser instance.
    >>> parser = get_option_parser(port=6060)
    >>> (options, args) = parser.parse_args(['-f', '/var/run/pid.file', '-g', 'admin', '-u', 'username'])
    >>> options.pidfile
    '/var/run/pid.file'
    >>> options.group
    'admin'
    >>> options.user
    'username'
    >>> options.port
    6060
    """

    if not default_pid_pathname:
        default_pid_pathname = DEFAULT_PID_PATHNAME

    option_parser = optparse.OptionParser()
    option_parser.add_option('-f', '--pidfile', metavar='FILE',
                             action='store', type='string', dest='pidfile',
                             default=default_pid_pathname,
                             help='Pathname to PID file')
    option_parser.add_option('-g', '--group', metavar='GRP',
                             action='store', type='string', dest='group',
                             default=DEFAULT_GROUP,
                             help='group to own daemon process')
    option_parser.add_option('-u', '--user', metavar='USER',
                             action='store', type='string', dest='user',
                             default=DEFAULT_USER,
                             help='User to own daemon process')
    if port:
        option_parser.add_option('-p', '--port', metavar='PORT',
                                 action='store', type='int', dest='port',
                                 default=port,
                                 help='Port to bind')
    return option_parser


def daemonize(mainfunc, mainfunc_args, pid_pathname,
              user=None, group=None, redirect=True):
    """Function for 'daemonizing' a process.

    It uses UNIX double-fork magic.  See 'Advanced Programming in
    the UNIX Environment' by Stevens (ISBN 0201563177).
    ***WARNING: This function is hostile.  It calls sys.exit, os.chdir
                and other system-level functions.  Do not invoke this
                unless you intend to daemonize a process.
    Note: parameter 'wide_pid' has gone.  It was related to a bug in rc.d in
    FreeBSD 6.3, which is fixed in FreeBSD 7.3 already.

    :param mainfunc: The main function of your program.
    :param mainfunc_args: The tuple of arguments to the main function.
    :param pid_pathname: The path to the PID file.
    :param user: The user who should own the daemon process.
    :param group: The group which should own the daemon process.
    :param redirect: True if stderr and stdout should be redirected.
    """

    assert callable(mainfunc), 'mainfunc argument is not a callable object.'
    assert pid_pathname is not None, 'Path to PID file must not be None.'

    __first_fork()
    # Now we are in the first fork.

    # decouple from parent environment
    os.chdir('/') # don't prevent unmounting
    os.setsid()
    os.umask(0002)

    # Reopen stdin, stdout, stderr as /dev/null.
    fd = os.open(os.devnull, os.O_RDWR)
    for new_fd in (0, 1, 2):
        os.dup2(fd, new_fd)
    if fd > 2:
        os.close(fd)

    if user:
        uid = pwd.getpwnam(user).pw_uid
    else:
        uid = None

    if group:
        gid = grp.getgrnam(group).gr_gid
    else:
        gid = None

    __second_fork(pid_pathname, uid, gid,)
    # Now we are in the second fork.

    try:
        # drop group privileges, if necessary
        if gid:
            os.setgid(gid)
        # drop user privileges, if necessary -- if we are going to
        # drop priveleges, make sure we set up our supplemental groups
        if uid:
            sup_groups = setuid.get_user_groups(user).keys()
            # We need to ensure that the primary group stays first in the
            # list, or else we end up changing the primary group.
            current_gid = os.getgid()
            if current_gid in sup_groups:
                sup_groups.remove(current_gid)

            sup_groups.insert(0, current_gid)
            os.setgroups(sup_groups)
            os.setuid(uid)

        # redirect outputs to a logfile
        if redirect:
            logger = logging.getLogger(mainfunc.__module__)
            oldstdout = sys.stdout
            oldstderr = sys.stderr
            sys.stdout = _StdLog(logger, logging.INFO)
            sys.stderr = _StdLog(logger, logging.WARNING)
    except:
        os.remove(pid_pathname)
        raise

    # start the user program
    try:
        mainfunc(*mainfunc_args)
    except Exception as err:
        logging.exception('%s occurred: %s', type(err), err)
        raise
    finally:
        logging.shutdown()
        if redirect:
            sys.stdout = oldstdout
            sys.stderr = oldstderr


def __first_fork():
    """Create a new fork and exit the parent process."""
    try:
        pid = os.fork()
        if pid > 0:
            # exit first parent
            sys.exit(0)
    except OSError as err:
        sys.stderr.write('Fork #1 failed: %s (%s)' % (err.errno, err))
        sys.exit(1)


def __second_fork(pid_pathname, uid, gid):
    """Helper for creating a new fork.

    Changes user and group of pid directory if needed.  The pid directory
    is created and the child pid is written to a file in the directory.

    :param pid_pathname: The path to the PID file.
    :param uid: Id of the user who should own the daemon process.
    :param gid: Id of the group which  should own the daemon process.
    """
    try:
        pid = os.fork()

        # Have the child write a pid file.
        if pid == 0:
            # Create the pid directory.
            try:
                pid_dirname = os.path.dirname(pid_pathname)
                if not os.path.isdir(pid_dirname):
                    os.makedirs(pid_dirname)
                    # chown to user so user can create files.
                    # N.B. we ONLY do the final path segment and ONLY if we
                    # created the dir - don't want to chown things like /tmp!
                    if gid:
                        os.chown(pid_dirname, -1, gid)
                    if uid:
                        os.chown(pid_dirname, uid, -1)

                # Print the pid of the child fork to the pid file.
                fhandle = open(pid_pathname, 'w')
                try:
                    fhandle.write('%s' % os.getpid())
                    if gid:
                        os.chown(pid_pathname, -1, gid)
                    if uid:
                        os.chown(pid_pathname, uid, -1)
                finally:
                    fhandle.close()
            except Exception as err:
                logging.exception('Error writing pid file: %s', err)
                sys.exit(1)

        # Have the parent exit.
        elif pid > 0:
            sys.exit(0)
    except OSError as err:
        sys.stderr.write('Fork #2 failed: %s (%s)' % (err.errno, err))
        sys.exit(1)


class _StdLog(object):

    """File-like object for trapping stderr and stdout."""

    def __init__(self, logger, lvl):
        """Initialize _StdLog instance.

        :param logger: Logger instance.
        :param lvl: Verbosity level of the logging.
        """
        self.logger = logger
        self.lvl = lvl

    def write(self, msg):
        """Log a message with an appropriate verbosity level.

        :param msg: A message to be logged.
        """
        s = msg.rstrip()
        if s:
            self.logger.log(self.lvl, s)


if __name__ == '__main__':
    import doctest
    doctest.testmod()

