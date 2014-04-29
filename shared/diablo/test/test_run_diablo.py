"""Unit tests for run_diablo.py

:Authors: scottrwi
$Id: //prod/main/_is/shared/python/diablo/test/test_run_diablo.py#8 $
"""

import logging
import logging.handlers
import optparse
import os
import signal
import threading
import time

import unittest2 as unittest

import shared.diablo.run_diablo

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)



class TestNodeMonitor(unittest.TestCase):

    def setUp(self):
        self.addCleanup(setattr,
            shared.diablo.run_diablo.shared.process.processutils,
            'MonitoredProcess',
            shared.diablo.run_diablo.shared.process.processutils.MonitoredProcess)

        shared.diablo.run_diablo.shared.process.processutils.MonitoredProcess =\
                FakeMonitoredProcess

    def test_regular_program(self):

        args = ['exe', 'arg']
        max_kill_delay = 10
        pipe_fds = False

        monitor = shared.diablo.run_diablo.NodeMonitor(args, 1, max_kill_delay,
                                                       pipe_fds)
        self.send_kill()
        monitor()

        expected_args = args + ['--node-id', '0']
        self.assertListEqual(TestNodeMonitor.args, expected_args)
        self.assertEqual(TestNodeMonitor.pipe_stdout, pipe_fds)
        self.assertEqual(TestNodeMonitor.pipe_stdout, pipe_fds)
        self.assertEqual(TestNodeMonitor.sig, signal.SIGINT)
        self.assertEqual(TestNodeMonitor.max_kill_delay, max_kill_delay)

    def send_kill(self):
        th = threading.Thread(target=self.delayed_kill)
        th.daemon = True
        th.start()

    def delayed_kill(self):
        time.sleep(0.1)
        os.kill(os.getpid(), signal.SIGINT)


class FakeMonitoredProcess(object):

    name = 'fake' # Needed for log call inside the NodeMonitor

    def __init__(self, name, args, pipe_stdout, pipe_stderr):
        TestNodeMonitor.args = args
        TestNodeMonitor.pipe_stdout = pipe_stdout
        TestNodeMonitor.pipe_stderr = pipe_stderr

    def start(self):
        pass

    def stop(self, sig, max_kill_delay):
        TestNodeMonitor.sig = sig
        TestNodeMonitor.max_kill_delay = max_kill_delay


class TestChildMethod(unittest.TestCase):

    def setUp(self):
        self.addCleanup(setattr, shared.diablo.run_diablo,
                        'config_child_logging',
                        shared.diablo.run_diablo.config_child_logging)

        shared.diablo.run_diablo.config_child_logging = fake_method

        self.addCleanup(setattr, shared.diablo.run_diablo.sys, 'exit',
                        shared.diablo.run_diablo.sys.exit)

        shared.diablo.run_diablo.sys.exit = fake_exit

    def call_child(self, daemon_class):
        class MockOptions:
            node_id = 0

        shared.diablo.run_diablo.child(daemon_class, {}, '', 'foo',
                                       MockOptions())
        os.kill(os.getpid(), signal.SIGINT)

    def test_run(self):
        self.call_child(FakeDiablo)
        self.assertEqual(TestChildMethod.ret_code, 0)
        self.assert_(TestChildMethod.reason.endswith(str(signal.SIGINT)))

    def test_run_exc(self):
        self.call_child(FakeDiabloExc)
        self.assertEqual(TestChildMethod.ret_code, 2)
        self.assert_(TestChildMethod.reason.endswith(str(signal.SIGINT)))


def fake_exit(code):
    TestChildMethod.ret_code = code


class TestConfigLogging(unittest.TestCase):

    def setUp(self):
        self.log = logging.getLogger()
        self.log.handlers = []

    def tearDown(self):
        self.log.handlers = []
        self.log.setLevel(LOG_LEVEL)

    def log_verify(self, args, handler, level, config_child):
        options = \
            shared.diablo.run_diablo.get_options_from_command_line(FakeDiablo,
                                                                   args)
        if config_child:
            shared.diablo.run_diablo.config_child_logging(options, 'foo')
        else:
            shared.diablo.run_diablo.config_parent_logging(options, 'foo')

        self.assertIsInstance(self.log.handlers[0], handler)
        self.assertEqual(self.log.level, level)

    ########################################
    # Child logging config tests
    ########################################

    def test_defaults(self):
        args = []
        self.log_verify(args, logging.StreamHandler, logging.INFO, True)

    def test_verbose(self):
        args = ['-v']
        self.log_verify(args, logging.StreamHandler, logging.DEBUG, True)

    def test_quiet(self):
        args = ['-q']
        self.log_verify(args, logging.StreamHandler, logging.WARN, True)

    def test_syslog(self):
        args = ['-s']
        self.log_verify(args, logging.handlers.SysLogHandler, logging.INFO, True)

    def test_syslog_quiet(self):
        args = ['-s', '-q']
        self.log_verify(args, logging.handlers.SysLogHandler, logging.WARN, True)

    def test_syslog_verbose(self):
        args = ['-s', '-v']
        self.log_verify(args, logging.handlers.SysLogHandler, logging.DEBUG, True)

    def test_file_config(self):
        shared.diablo.run_diablo._config_log_conf = self.my_file_log_config

        FINAME = '/thisfiledoesntexist'
        args = ['-l', FINAME]
        options = \
            shared.diablo.run_diablo.get_options_from_command_line(FakeDiablo,
                                                                   args)
        shared.diablo.run_diablo.config_child_logging(options, 'foo')

        self.assertEqual(self.called_fileconfig, FINAME)

    def my_file_log_config(self, finame):
        self.called_fileconfig = finame

    ########################################
    # Parent logging config tests
    ########################################

    def test_parent_default(self):
        args = ['-p']
        self.log_verify(args, logging.StreamHandler, logging.INFO, False)

    def test_parent_verbose(self):
        args = ['-p', '-v']
        self.log_verify(args, logging.StreamHandler, logging.DEBUG, False)

    def test_parent_quiet(self):
        args = ['-p', '-q']
        self.log_verify(args, logging.StreamHandler, logging.WARN, False)

    def test_daemon(self):
        args = ['-d']
        self.log_verify(args, logging.handlers.SysLogHandler, logging.INFO, False)

    def test_daemon_quiet(self):
        args = ['-d', '-q']
        self.log_verify(args, logging.handlers.SysLogHandler, logging.WARN, False)

    def test_daemon_verbose(self):
        args = ['-v', '-d']
        self.log_verify(args, logging.handlers.SysLogHandler, logging.DEBUG, False)


class TestParentFunc(unittest.TestCase):

    def setUp(self):

        if 'CONFROOT' in os.environ:
            self.addCleanup(os.environ.__setitem__, 'CONFROOT',
                            os.environ['CONFROOT'])
        else:
            self.addCleanup(os.environ.__delitem__, 'CONFROOT')

        os.environ['CONFROOT'] = '/tmp/does/not/exist/hopefully'

        self.addCleanup(setattr, shared.diablo.run_diablo, 'NodeMonitor',
                        shared.diablo.run_diablo.NodeMonitor)

        shared.diablo.run_diablo.NodeMonitor = MyNodeMonitor

        self.addCleanup(setattr,
                        shared.diablo.run_diablo.shared.process.daemon,
                        'daemonize',
                        shared.diablo.run_diablo.shared.process.daemon.daemonize)

        shared.diablo.run_diablo.shared.process.daemon.daemonize = fake_daemonize

    def tearDown(self):
        logging.getLogger().setLevel(LOG_LEVEL)

    def call_parent(self, conf, args):
        options = \
            shared.diablo.run_diablo.get_options_from_command_line(FakeDiablo,
                                                                   args)

        real_conf = {}
        for k,v in conf.iteritems():
            real_conf['diablo.' + k] = v

        shared.diablo.run_diablo.parent(real_conf, 'diablo', 'app', options)

    def test_parent(self):
        args = ['-p']
        conf = {}
        self.call_parent(conf, args)
        self.assertListEqual(TestParentFunc.args, [])
        self.assertEqual(TestParentFunc.num_nodes, 1)
        self.assertEqual(TestParentFunc.max_kill_delay, 60)
        self.assertEqual(TestParentFunc.pipe_fds, False)

    def test_parent_with_config(self):
        args = ['-p']
        conf = {'max_kill_delay': 100, 'num_nodes': 10}
        self.call_parent(conf, args)
        self.assertListEqual(TestParentFunc.args, [])
        self.assertEqual(TestParentFunc.num_nodes, 10)
        self.assertEqual(TestParentFunc.max_kill_delay, 100)
        self.assertEqual(TestParentFunc.pipe_fds, False)

    def test_daemon(self):
        args = ['-d']
        conf = {}
        self.call_parent(conf, args)

        # Since $CONFROOT/log.conf is not readable, it reverts to -s
        self.assertListEqual(TestParentFunc.args, ['-s'])

        self.assertEqual(TestParentFunc.num_nodes, 1)
        self.assertEqual(TestParentFunc.max_kill_delay, 60)
        self.assertEqual(TestParentFunc.pipe_fds, True)

        self.assertEqual(TestParentFunc.pidfile , '/tmp/app.pid')
        self.assertEqual(TestParentFunc.user, None)
        self.assertEqual(TestParentFunc.group, None)

    def test_daemon_with_config(self):
        args = ['-d']
        conf = {'max_kill_delay': 100, 'num_nodes': 10, 'pid_dir': '/foo'}
        self.call_parent(conf, args)

        # Since $CONFROOT/log.conf is not readable, it reverts to -s
        self.assertListEqual(TestParentFunc.args, ['-s'])

        self.assertEqual(TestParentFunc.num_nodes, 10)
        self.assertEqual(TestParentFunc.max_kill_delay, 100)
        self.assertEqual(TestParentFunc.pipe_fds, True)

        self.assertEqual(TestParentFunc.pidfile , '/foo/app.pid')

    def test_cannot_change_user(self):
        args = ['-d']
        conf = {'user': 'dummy_user', 'group': 'dummy_group'}

        try:
            self.call_parent(conf, args)
        except Exception:
            pass
        else:
            self.assert_(False, 'Did not throw exception when changing'
                         'user when not root')

    def test_child_log_options_file(self):
        args = ['-p', '-v', '--logconf', 'logfile']
        conf = {}
        self.call_parent(conf, args)

        # Should not pass verbose to child because of --logconf
        self.assertListEqual(TestParentFunc.args, args[2:])

        self.assertEqual(TestParentFunc.num_nodes, 1)
        self.assertEqual(TestParentFunc.max_kill_delay, 60)
        self.assertEqual(TestParentFunc.pipe_fds, False)

    def test_child_log_options_syslog(self):
        args = ['-p', '-s', '-v']
        conf = {}
        self.call_parent(conf, args)

        # Should not pass verbose to child because of --logconf
        self.assertListEqual(TestParentFunc.args, args[1:])

        self.assertEqual(TestParentFunc.num_nodes, 1)
        self.assertEqual(TestParentFunc.max_kill_delay, 60)
        self.assertEqual(TestParentFunc.pipe_fds, False)


class MyNodeMonitor(object):
    def __init__(self, args, num_nodes, max_kill_delay, pipe_fds):
        TestParentFunc.args = args[2:]
        TestParentFunc.num_nodes = num_nodes
        TestParentFunc.max_kill_delay = max_kill_delay
        TestParentFunc.pipe_fds = pipe_fds

    def __call__(self):
        pass


class FakeDiablo(object):
    VERSION = 'N/A'

    def __init__(self, *args, **kwargs):
        pass

    def run(self):
        pass

    def stop_daemon(self, reason=None):
        TestChildMethod.reason = reason


class FakeDiabloExc(FakeDiablo):
    def run(self):
        raise Exception()


def fake_daemonize(func, args, pidfile, user, group):
    TestParentFunc.pidfile = pidfile
    TestParentFunc.user = user
    TestParentFunc.group = group


def fake_method(*args, **kwargs):
    pass


if __name__ == '__main__':
    unittest.main()
