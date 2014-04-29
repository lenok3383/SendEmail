"""Unit test for common/processutils.py

:Status: $Id: //prod/main/_is/shared/python/process/test/test_processutils.py#7 $
:Author: jhaight, duncan
"""

import logging
import os
import signal
import sys
import time
import unittest2 as unittest

from shared.process import processutils

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)


class TestMonitoredProcess(unittest.TestCase):

    def test_start_stop(self):
        """test start, kill (restart) & stop"""
        p = processutils.MonitoredProcess(
            'vmstat', args=('/usr/bin/vmstat', '1',), restart_delay=.1)
        timer = time.time()
        p.start()
        time.sleep(0.2)
        self.assertTrue(p.is_alive())
        time.sleep(0.2)
        p.kill(signal.SIGTERM)
        time.sleep(0.3)
        self.assertTrue(p.is_alive())
        p.stop(max_kill_delay=3)
        self.assertTrue(not p.is_alive())
        self.assertTrue(time.time() - timer < 2)

    def test_restart(self):
        """Test start/stop of a process that won't keep running.

        The process should get restarted automatically.
        """
        def start_callback():
            self.starts += 1

        self.starts = 0
        p = processutils.MonitoredProcess(
            'echo', args=('echo','hi'), restart_delay=0.01,
            pipe_stdout=True, pipe_stderr=False, log_pid=True,
            start_callback=start_callback)
        p.start()
        time.sleep(0.1)
        p.stop()

        self.assertGreater(self.starts, 1)

    def test_silent_kill(self):
        """Test killing a process that emits no output"""
        p = processutils.MonitoredProcess('sleep 60', args=('sleep', '60'))
        timer = time.time()
        p.start()
        time.sleep(0.5)
        p.stop()
        self.assert_(not p.isAlive())
        self.assert_(time.time() - timer < 0.8)

    def test_died_cpu_eat(self):
        """Test for bugid 45858, consuming cpu+mem in loop"""
        p = processutils.MonitoredProcess('false', args=('false',))
        start = time.clock()
        p.start()
        time.sleep(1)
        p.stop()
        stop = time.clock()
        self.assert_(stop - start < .05)

    def test_unexpected_kill(self):
        # Make sure an unexpected (external) signal is handled properly.
        p = processutils.MonitoredProcess('sleep 60', args=('sleep', '60'),
                                          restart_delay=0.0001)
        p.start()
        time.sleep(0.2)
        os.kill(p._MonitoredProcess__child.pid, signal.SIGTERM)
        time.sleep(0.1)
        self.assert_(p.isAlive())
        p.stop()
        self.assert_(not p.isAlive())


class ExecuteTest(unittest.TestCase):

    """Some simple tests for execute and iter_execute
    """

    def test_iter_execute_success(self):
        # Test successful iter_execute.
        success = False
        for line in processutils.iter_execute('ls /bin'):
            if line == 'sh\n':
                success = True

        self.assertTrue(success, 'iter_execute(\'ls /bin\') failed!')

    def test_iter_execute_broken(self):
        # Test broken iter_execute.
        try:
            for line in processutils.iter_execute(
                'ls /this_does_not_exist'):
                pass
            self.fail('iter_execute(\'ls /this_does_not_exist\') should have '
                      'failed!')
        except processutils.ExecuteError, e:
            # This isn't pretty...
            self.assertTrue('No such file or directory' in e[0][1])

    def test_execute_success(self):
        self.assertEqual(
            processutils.execute('echo foo', read_output=True), 'foo\n',
            'execute(\'echo foo\') failed!')

    def test_execute_hang(self):
        # This is being executed through the shell, so we need to quote things.
        # We'll convert ' to " to make things easier.
        hang_test = ' '.join('\'%s\'' % (arg.replace('\'', '"'),)
                             for arg in HANG_TEST)

        # The test is trying to see if this will hang.
        out = processutils.execute(
            hang_test, read_output=True)

        self.assertEqual(len(out), 1024*4096)

HANG_TEST = [
    processutils.get_python_executable(), '-E', '-c',
r"""import sys
import time
time.sleep(0.005)
sys.stderr.write('hello\nblah')
sys.stderr.flush()
for i in xrange(1024):
    sys.stdout.write('x' * 4095 + '\n')
sys.stdout.flush()
time.sleep(0.005)
"""]


class TestTimedSubprocess(unittest.TestCase):

    def test_basic(self):
        stdout = list()
        stderr = list()
        start = time.time()
        ret = processutils.timed_subprocess(
            ['echo', 'foo'], run_timeout=2, kill_timeout=2,
            stdout_func=stdout.append, stderr_func=stderr.append)
        self.assertTrue(time.time() - start < 1,
                        'Took too long to execute "echo foo"')
        self.assertEqual(stdout, ['foo\n'])
        self.assertEqual(stderr, [])

        # Shouldn't have needed a kill.
        self.assertEqual(ret, 0)

    def test_fail(self):
        ret = processutils.timed_subprocess(
            ['false'], run_timeout=2, kill_timeout=2)
        self.assertEqual(ret, 1)

    def test_execute_hang(self):
        out = list()

        # This will hang if it "fails"...
        exit_code = processutils.timed_subprocess(
            HANG_TEST, run_timeout=5, kill_timeout=5,
            stdout_func=out.append, stderr_func=out.append)

        # Shouldn't have needed a kill.
        self.assertEqual(exit_code, 0)

        # Check the output.  We do line based reads, so the first full line we
        # saw was "hello\n" written to stderr, then the x's, then a "blah" with
        # no trailing \n.

        self.assertEqual(out[0], 'hello\n')
        self.assertEqual(out[-1], 'blah')
        self.assertEqual(len(out), 1024 + 2)
        for line in out[1:-1]:
            self.assertEqual(line, 'x' * 4095 + '\n')

    def test_kill(self):
        start = time.clock()
        exit_code = processutils.timed_subprocess(
            ['sleep', '5'], run_timeout=0.1, kill_timeout=0.1)
        end = time.clock()
        self.assertTrue(end - start < 0.15, 'sleep 5 not killed fast enough')

        # Should have needed a term.
        self.assertEqual(exit_code, -signal.SIGTERM)

    def test_os_error(self):
        self.assertRaises(
            OSError,
            processutils.timed_subprocess,
            ['nonexistant file'], 1, 1)

    def test_need_kill(self):
        args = [processutils.get_python_executable(), '-E', '-c',
"""import time
import signal
signal.signal(signal.SIGTERM, signal.SIG_IGN)
while True:
    time.sleep(1)
"""]

        start = time.clock()
        exit_code = processutils.timed_subprocess(
            args, run_timeout=0.5, kill_timeout=0.5)
        end = time.clock()
        self.assertTrue(end - start < 1.2, 'python not killed fast enough')

        # Should have needed a kill, since term is ignored.
        self.assertEqual(exit_code, -signal.SIGKILL)

    def test_complete_output(self):
        msg = 'blah' * 1024 + '\n'
        python_prog = """import signal
import errno
import sys
_continue = True
def stop(signum, frame):
    global _continue
    _continue = False

signal.signal(signal.SIGTERM, stop)
while _continue:
    try:
        sys.stdout.write('''%s''')
    except IOError, e:
        if e.errno != errno.EINTR:
            raise
""" % (msg,)

        # Make sure that we get complete output lines.
        output = []
        start = time.clock()
        exit_code = processutils.timed_subprocess(
            [processutils.get_python_executable(), '-E', '-u', '-c',
            python_prog], run_timeout=0.4, kill_timeout=5,
            stdout_func=output.append, stderr_func=sys.stderr.write)
        end = time.clock()
        self.assertTrue(end - start < 0.8, 'python not killed fast enough')

        self.assertTrue(len(output) > 10, 'only %d output lines received' %
                    (len(output),))

        for line in output:
            self.assertEqual(line, msg)

        # Python handles the signal and exits normally.
        self.assertEqual(exit_code, 0)

if __name__ == '__main__':
    unittest.main()

