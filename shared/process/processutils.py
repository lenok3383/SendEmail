"""Several Utilities for managing sub-processes.

Provides several functions to facilitate running subprocesses, handling
standard output and standard error, killing processes if they time out, etc.

Also provides a few utility functions for python executables.

:Status: $Id: $
:Authors: ted, jhaight, scottrwi, duncan, ereuveni
"""
import collections
import errno
import inspect
import logging
import os
import select
import signal
import subprocess
import sys
import threading
import time

class ExecuteError(Exception):
    """Raised when execute() or iter_execute() fails.

    This is thrown when the command can not be executed, or when it returns a
    non-zero exit code.
    """
    pass


def find_source_path(source_obj):
    """Return the full path to the source file which defines the object."""
    return os.path.abspath(inspect.getsourcefile(source_obj))


def get_python_executable():
    """Return the full path to the python interpreter executable."""
    return sys.executable


class MonitoredProcess(threading.Thread):

    """Thread wrapper for a sub-process.

    Starts a process, copies data from stdout and stderr to logs, restarts
    the process when/if it dies.  Shuts the process down by killing it with
    the desired signal.

    Synopsis:

    args = (get_python_executable(), 'pythonscript.py')
    process = MonitoredProcess(name='My python script', args=args)
    process.start()
    if time_to_die:
       process.stop(sig=signal.SIGTERM, max_kill_delay=5)
    """

    def __init__(self, name, args, restart_delay=5, cwd=None,
                 pipe_stdout=True, pipe_stderr=True,
                 start_callback=None, env=None,
                 log_pid=True):
        """Specify the process to be monitored and how to handle it.

        :param name: a name for the process
        :param args: argument list for os.spawnv()
        :param restart_delay: seconds to wait before restart if process dies
        :param cwd: Current Working Directory (cd) for process
        :param pipe_stdout: pass stdout to log (True|False)
        :param pipe_stderr: pass stderr to log (True|False)
        :param start_callback: method to call when process is started or
                               restarted
        :param env: Dictionary of environment variables to pass to subprocess
        :param log_pid: If True log messages will show pid and <stdout|stderr>
                        on each line.  If False it will just be message itself
        """
        threading.Thread.__init__(self)
        self.name = name
        self.__log = logging.getLogger(self.name)
        self.__args = args
        self.__cwd = cwd
        # This is the default signal in case of self-shutdown.
        self.__sig = signal.SIGTERM
        self.__max_kill_delay = 30
        self.__pipe_stdout = pipe_stdout
        self.__pipe_stderr = pipe_stderr
        self.__restart_delay = restart_delay
        self.__shutdown = threading.Event()
        self.__killed = False
        self.__died = False
        self.restarts = -1
        self.last_start = None
        self.__start_callback = start_callback
        self.__env = env
        self.__log_pid = log_pid
        self.__stop_lock = threading.Lock()

    def getName(self):
        """Returns string with class name, process name and args."""
        return 'MonitoredProcess: %s (%s)' % (self.name, ' '.join(self.__args))

    def run(self):
        """The main run loop - do not invoke directly - runs from threading

        Process is started, restarted and stopped here.
        Passes output to logs.
        Restarts process if it dies.
        """
        while not self.__shutdown.isSet():
            self.__start_process()
            try:
                # Process started, pass output until stoped.
                while not self.__died and not self.__shutdown.isSet():
                    self.__child.safe_wait(
                        stop_condition=self.__shutdown.isSet)
                    self.__waitpid()
            except select.error, e:
                # EINTR is normal (a feature) when pid is killed.
                if e[0] != errno.EINTR:
                    raise
            except:
                self.__log.warn('Exception from process %s',
                                self.getName(), exc_info=1)

            # Restarting, not a normal stop.
            if not self.__shutdown.isSet():
                self.__stop_process()
                self.__shutdown.wait(self.__restart_delay)

    def stop(self, sig=signal.SIGTERM, max_kill_delay=30):
        """Stop the process permanently."""
        self.__sig = sig
        self.__max_kill_delay = max_kill_delay
        self.__shutdown.set()
        self.__stop_process()
        self.join()

    def kill(self, sig=signal.SIGTERM):
        """Stop the process, but let it restart."""
        self.__sig = sig
        self.__stop_process()

    def is_alive(self):
        """Returns true if the process is running."""
        return self.__child.pid and self.__child.returncode is None

    def __start_process(self):
        """Start or re-start the process"""

        try:
            child = _SubprocessWrapper(
                args=self.__args, cwd=self.__cwd, env=self.__env,
                stdout_func=self.__log_stdout, stderr_func=self.__log_stderr)
        except:
            self.__log.exception('Error starting process %s', self.getName())
            raise
        self.__died = False
        self.__killed = False
        self.__child = child

        self.restarts += 1

        if self.__start_callback:
            self.__start_callback()

        self.last_start = time.time()

        self.__log.info('Start %s', self)

    def __stop_process(self):
        """Stop the process, wait for it to die.

        This will kill -9 if process does not stop in time!
        """

        self.__stop_lock.acquire()
        try:
            if not self.__killed and not self.__died:
                self.__killed = True
                self.__child.stop(term_signal=self.__sig,
                                  kill_delay=self.__max_kill_delay)
        finally:
            self.__waitpid()
            self.__stop_lock.release()

    def __waitpid(self):
        """Check for process death, don't actually wait, just poll.

        return True if it is dead, False otherwise
        """
        if not self.__died:
            rcode = self.__child.poll()
            if rcode is not None:
                if rcode < 0:
                    self.__log.info('pid %d killed by signal: %d',
                                    self.__child.pid, -1 * rcode)
                else:
                    self.__log.info('pid %d exited: %d',
                                    self.__child.pid, rcode)
                self.__died = True
        return self.__died

    def __log_stdout(self, msg):
        self.__generic_log(msg, should_log=self.__pipe_stdout,
                           log_name='stdout', stream=sys.stdout)

    def __log_stderr(self, msg):
        self.__generic_log(msg, should_log=self.__pipe_stderr,
                           log_name='stderr', stream=sys.stderr)

    def __generic_log(self, msg, should_log, log_name, stream):
        if should_log:
            # Strip trailing newline.
            msg = msg.strip()
            if self.__log_pid:
                self.__log.info('(PID %s %s) %s',
                                self.__child.pid, log_name, msg)
            else:
                self.__log.info(msg)
        else:
            stream.write(msg)

def iter_execute(command, command_input=None, timeout=None,
                 stop_condition=None, cwd=None,
                 terminate_signal=signal.SIGTERM,
                 kill_delay=0, env=None):
    """Executes a shell command, returning output line-by-line.

    Raises ExecuteError if the command fails.  It generates an iterator to
    retrieve the output (standard output and standard error) of the command
    line-by-line.

    Because standard output and standard error are being returned using the
    same iterator, it is possible the order of messages from standard output
    and standard error are mixed.

    :param command:  Shell command to be executed directly;
                     either a sequence or a string to be .split()
    :param command_input:  Data to write to process's stdin.
    :param timeout:  Maximum number of seconds to wait for child's output.
    :param stop_condition: (optional) function that will return True when the
      command should be killed. (Default: None; not used.)
    :param cwd: if not None, the current directory will be changed to cwd
                before the child is executed.
    :param terminate_signal: the signal to send if the child needs to
                be terminated due to timeout
    :param kill_delay: the time (in seconds) to wait before attempting to
                SIGKILL the process if it does not exit after the first
                signal. (Default: 0; disables SIGKILL, but is not recommended.)
    :returns: generator yielding a line of output (standard error or standard
        output) at a time.
    """
    # Store last errors for debugging.
    max_errors_to_save = 20

    if isinstance(command, basestring):
        # TODO: use shlex.split()?
        command = command.split()

    child = _SubprocessWrapper(args=command, cwd=cwd, stdin=subprocess.PIPE,
            env=env)
    if command_input:
        child.stdin.write(command_input)
    child.stdin.close()

    errors = collections.deque()

    for fh, msg in child.iter_pipes(timeout=timeout,
                                    stop_condition=stop_condition):

        # TODO: It might be useful in some use cases to have the caller
        # differentiate between messages from stdout and stderr.  This could be
        # achieved with backwards compatibility using a subclass of UserString
        # (acts like a string in almost every way).

        if fh == child.STDOUT:
            yield msg
        if fh == child.STDERR:
            errors.append(msg)
            if len(errors) > max_errors_to_save:
                errors.popleft()
            yield msg

    child.poll()
    exit_code = child.returncode
    if exit_code is None:
        if kill_delay > 0:
            signal_used = child.stop(kill_delay=kill_delay,
                                     term_signal=terminate_signal)
        else:
            # For backwards compatibility, don't ensure it's dead.  This
            # probably should be deprecated.
            os.kill(child.pid, terminate_signal)
            signal_used = terminate_signal

        raise ExecuteError(
            (-999, 'command "%s" ran too long, killed with signal %s' %
             (command, signal_used)))

    assert exit_code is not None
    if exit_code:
        if exit_code > 0:
            exit_cause = 'Exit code %d' % (exit_code,)
        else:
            exit_cause = 'Terminated by signal %d' % (-exit_code,)
        raise ExecuteError((exit_code,
                'command "%s" failed.\n  %s. '
                'stderr: "%s"' % (command, exit_cause, ''.join(errors))))


def execute(command, read_output=False, command_input=None,
            timeout=None, terminate_signal=signal.SIGTERM, env=None):
    """Executes a shell command.  Raises ExecuteError if the command fails.

    :param command: Shell command to be executed via /bin/sh.
    :param read_output: Boolean to return the output of the command.
    :param command_input: String to use as input to the command.
    :param terminate_signal: Signal to use if the child needs to be
                terminated due to timeout
    :return: Standard output of the command.
    """
    stderr_msgs = []
    stdout_func = None
    if read_output:
        stdout_msgs = []
        stdout_func = stdout_msgs.append

    stdin = None
    if command_input:
        stdin = subprocess.PIPE

    child = _SubprocessWrapper(args=command, shell=True, stdin=stdin,
                               stdout_func=stdout_func,
                               stderr_func=stderr_msgs.append,
                               env=env, use_chunks=True)
    if command_input:
        child.stdin.write(command_input)
        child.stdin.close()

    child.safe_wait(timeout=timeout)

    # Check if we finished or timed out.
    if child.returncode is None:
        os.kill(child.pid, terminate_signal)
        raise ExecuteError((-1,
            'command "%s" failed timeout exceeded' % (command,)))

    if child.returncode:
        raise ExecuteError((child.returncode,
                'command: "%s" failed.  exit code: %d. '
                'stderr: "%s"' % (command, child.returncode,
                                  ''.join(stderr_msgs))))
    if read_output:
        return ''.join(stdout_msgs)


def timed_subprocess(args, run_timeout, kill_timeout, stdin_data=None,
                     stdout_func=None, stderr_func=None, env=None):
    """Runs a subprocess until a timeout occurs.

    If the process runs for longer than `run_timeout`, it will be killed with
    SIGTERM.  If it does not die within `kill_timeout` after that, it will be
    killed with SIGKILL.

    Messages emitted by subprocesses on standard output and standard error will
    be passed (one line at a time, with a trailing newline) to the functions
    passed in as ``stdout_func`` and ``stderr_func``, respectively. If not
    provided (or ``None``), the messages will be discarded.

    It is important that ``stdout_func`` and ``stderr_func`` be fast,
    otherwise, it is possible that timeouts will occur because the data is not
    being processed fast enough.

    Note that if the process failes to execute (e.g. no such file or
    directory, permission denied, etc), OSError will be raised.

    :Parameters:
        - `args`: a list of command arguments to pass to Popen()
        - `run_timeout`: time the process is allowed to run for
        - `kill_timeout`: delay after SIGTERM before SIGKILL
        - `stdin_data`: data to write to subprocess standard input
        - `stdout_func`: callback function for subprocess standard output
        - `stderr_func`: callback function for subprocess standard error

    :Returns:
        Child process's exit code.
    """
    log = logging.getLogger('shared.process.processutils.timed_subprocess')

    child = _SubprocessWrapper(
        args=args, stdout_func=stdout_func, stderr_func=stderr_func,
        stdin=subprocess.PIPE, env=env)

    if stdin_data:
        child.stdin.write(stdin_data)
    child.stdin.close()

    child.safe_wait(run_timeout)
    if child.returncode is not None:
        return child.returncode

    log.warn('Timeout exceeded for subprocess %s', args)
    kill_sig = child.stop(kill_delay=kill_timeout)
    log.warn('Process killed with signal %d', kill_sig)

    return child.returncode


class _SubprocessWrapper(object):

    """Wrapper around subprocess.Popen to better handle stdout and stderr.

    This class acts as a subclass of subprocess.Popen using an attribute
    proxy. It also adds a few additional methods to support better (memory
    efficient, and deadlock-free) handling of stdout and stderr, and handle
    agressive killing of subprocesses.
    """

    STDOUT = 1
    STDERR = 2

    def __init__(self, stdout_func=None, stderr_func=None,
                 use_chunks=False, **kwargs):
        """Start a new subprocess.

        All keyword arguments are passed to subprocess.Popen(). ``close_fds``,
        ``stdout`` and ``stderr`` are automatically set, so they should not be
        provided.

        Data read from the subprocess while waiting for it to exit (using
        ``safe_wait()`` or ``stop()``) will be passed to callback functions if
        the are provided. Data from standard output will be passed to
        ``stdout_func``, and standard error will be passed to
        ``stderr_func``. The argument to these functions will be a single line
        of output (including terminating \n, if provided).

        :Parameters:
            - `stdout_func`: Callback function provided with each complete line
               read from the child's stdout.
            - `stderr_func`: Callback function provided with each complete line
               read from the child's stderr.
            - `use_chunks`: If set to True data will be yielded as chunks as
              they appear. Use it if there is not reason to split incoming
              data as separate messages. If incoming string is too long,
              splitting will cause tremendous performance degradation. Use
              this option to handle it by your own means.
        """

        self._use_chunks = use_chunks
        self._child = subprocess.Popen(close_fds=True,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       **kwargs)
        self._out = ''
        self._error = ''
        self._out_eof = False
        self._error_eof = False

        self._handlers = {self.STDOUT: stdout_func,
                          self.STDERR: stderr_func}

    # We're using the attribute proxy trick to fake inheritance.  We don't want
    # to directly inherit from subprocess.Popen since we don't know if that's
    # safe.
    def __getattr__(self, attr):
        return getattr(self._child, attr)

    def iter_pipes(self, timeout=None, stop_condition=None):
        """Iterate over standard output and standard error messages.

        Values generated are of the form (type, msg), where type is either
        _SubprocessWrapper.STDOUT or _SubprocessWrapper.STDERR and msg is a
        complete line of output, including a newline (if one was provided).

        Subsequent calls to this function will pick up where it left off. The
        generator will not exit until timeout is reached, or the subprocess
        exits.

        Data returned by this generator will not be passed to the callback
        functions ``stdout_func`` and ``stderr_func``.

        :Parameters:
            - `timeout`: If a timeout is provided, iteration will stop after
               that number of seconds have elapsed. Unprocessed data will be
               kept, allowing for repeated calls.
            - `stop_condition`: If provided, this function will exit if this
               returns True. (Must be consistent.)
        """
        poll = select.poll()
        if not self._out_eof:
            poll.register(self._child.stdout.fileno())
        if not self._error_eof:
            poll.register(self._child.stderr.fileno())

        start_time = time.time()

        # A somewhat complicated condition...
        def should_continue():
            if timeout is not None and time.time() - start_time > timeout:
                return False
            if stop_condition and stop_condition():
                return False
            return True

        while should_continue() and not (self._out_eof and self._error_eof):

            # If there are no events in 100 ms, we'll just go around the loop
            # again.  We use os.read() in this loop since it will return all
            # the data that is available, up to 1K without blocking, if there
            # is data available.  (self._child.stderr.read(n) will keep calling
            # read() until it gives us exactly n bytes back.)  As a result,
            # this should be faster than a loop with read(1)'s.

            try:
                poll_events = poll.poll(100)
            except select.error as e:
                if e[0] != errno.EINTR:
                    raise
                continue

            for fd, event in poll_events:

                if fd == self._child.stderr.fileno() and \
                       event & (select.POLLIN|select.POLLHUP):

                    data = os.read(self._child.stderr.fileno(), 65536)
                    if not data:
                        poll.unregister(self._child.stderr.fileno())
                        self._error_eof = True
                        if self._error:
                            yield (self.STDERR, self._error)
                            self._error = ''
                        continue

                    # Add to the data we have, keep the last item since it's
                    # unfinished.
                    self._error += data
                    lines = self._error.split('\n')
                    self._error = lines.pop()
                    for line in lines:
                        yield (self.STDERR, line + '\n')

                if fd == self._child.stdout.fileno() and \
                       event & (select.POLLIN|select.POLLHUP):

                    data = os.read(self._child.stdout.fileno(), 65536)
                    if not data:
                        poll.unregister(self._child.stdout.fileno())
                        self._out_eof = True
                        if self._out:
                            yield (self.STDOUT, self._out)
                            self._out = ''
                        continue

                    # Add to the data we have, keep the last item since it's
                    # unfinished.
                    if self._use_chunks:
                        yield (self.STDOUT, data)
                    else:
                        self._out += data
                        lines = self._out.split('\n')
                        self._out = lines.pop()
                        for line in lines:
                            yield (self.STDOUT, line + '\n')

        # Check if child has exited.
        self._child.poll()

        while should_continue() and self._child.returncode is None:
            # stdout and stderr are closed, let's wait to exit.
            time.sleep(0.05)
            self._child.poll()

    def safe_wait(self, timeout=None, stop_condition=None):
        """Waits for the process to exit while handling/discarding the buffers.

        Unlike wait(), this can use a timeout or a stop condition, and will
        never lead to deadlock caused by subprocesses filling the stdout/stderr
        buffer.

        Data read from the stdout/stderr buffers are passed to the
        ``stdout_func`` and ``stderr_func`` callback functions.

        :Parameters:
            - `timeout`: If a timeout is provided, the function will return
               after that number of seconds have elapsed. Unprocessed data will
               be kept, allowing for repeated calls.
            - `stop_condition`: If provided, this function will exit if this
               returns True. (Must be consistent.)
        """
        for fh, msg in self.iter_pipes(timeout=timeout,
                                       stop_condition=stop_condition):
            func = self._handlers.get(fh)
            if func:
                func(msg)

        return self._child.poll()

    def stop(self, kill_delay=30, term_signal=signal.SIGTERM):
        """Try to kill the process, resorting to SIGKILL if it does not exit.

        :Parameters:
            - `term_signal`: Signal to use for (gently) killing.
            - `kill_delay`: Time to wait before using SIGKILL.

        :Returns:
            Last kill signal sent, or None if process is already stopped.
        """
        # Make sure it's not already dead; we don't want to send spurious
        # signals if our PID is already reused.
        killed = None
        self._child.poll()
        if self._child.returncode is not None:
            return killed

        # Not dead yet.  Kill it!
        try:
            os.kill(self._child.pid, term_signal)
            killed = term_signal
        except OSError, e:
            if e.errno != errno.ESRCH:
                raise

        # Wait until it's dead, or for kill_delay.
        self.safe_wait(timeout=kill_delay)

        # Not dead yet?  SIGKILL in a loop.  If SIGKILL doesn't work, we're
        # in trouble.
        for _ in xrange(30):
            if self.returncode is not None:
                break
            try:
                os.kill(self._child.pid, signal.SIGKILL)
                killed = signal.SIGKILL
            except OSError, e:
                if e.errno != errno.ESRCH:
                    raise
            self.safe_wait(timeout=1)
        else:
            # Ran off the end of the loop!  SIGKILL didn't work!
            log = logging.getLogger('common.procesutils._SubprocessWrapper')
            log.error('Repeated SIGKILL did not kill PID %d', self._child.pid)

        return killed
