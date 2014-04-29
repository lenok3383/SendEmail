"""Unit test for DiabloBase class and app_thread decorator

:Authors: scottrwi
$Id: //prod/main/_is/shared/python/diablo/test/test_base.py#9 $
"""

import copy
import logging
import os
import signal
import threading
import time
import unittest2 as unittest

import shared.diablo.base


RUN_TIMEOUT = 5
TIMEOUT_SLICE = 0.1

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)

class TestDiabloRun(unittest.TestCase):

    def setUp(self):
        self.send_stop = False
        self.kill_wait = 0.0

        self.assertItemsEqual(
            threading.enumerate(), [threading.current_thread()],
            'Threads left over from previous test.  Cannot run.')

    def instantiate(self, cls):
        self.diablo = cls({}, 'diablo', 'test', 0)

        self.app_shutdown_actions = [self.diablo.app_shutdown,
                                     self.diablo._join_app_threads,
                                     self.diablo.stop_daemon]

        # Monkey patch in our own start_app to be able to send kill
        # after threads are started
        self.orig_start_app = self.diablo._start_app
        self.diablo._start_app = self.my_start_app

        self.orig_do_shutdown = self.diablo._do_shutdown_actions
        self.diablo._do_shutdown_actions = self.save_shutdown_actions

    def diablo_run(self, assert_exc=None):
        # Run is separate thread to ensure unit test does not hang
        th = threading.Thread(name='run', target=self.run_catch_errors,
                              args=(assert_exc,))
        th.start()

        # Try to join every TIMEOUT_SLICE until RUN_TIMEOUT is reached.
        # If thread hasn't joined by then, move along
        for i in xrange(int(RUN_TIMEOUT/TIMEOUT_SLICE)):
            th.join(TIMEOUT_SLICE)
            if not th.isAlive():
                break
        self.assertFalse(th.isAlive(), 'Run thread did not die.')

    def run_catch_errors(self, assert_exc):

        if assert_exc:
            self.assertRaises(assert_exc, self.diablo.run)
        else:
            self.diablo.run()

    def plugin_shutdowns(self):
        actions = [getattr(plugin, 'shutdown')
                   for plugin in self.diablo._plugins.values()]
        return actions

    def my_start_app(self):
        self.orig_start_app()

        if self.send_stop:
            time.sleep(self.kill_wait)
            self.diablo.stop_daemon()

    def test_regular_exit_with_stop(self):
        self.send_stop = True
        self.instantiate(RegularLoop)
        self.diablo.register_plugin(GoodPlugin)
        self.diablo_run()
        self.assertTrue(self.diablo.loop_exited, 'App loop did not exit')
        self.assertListEqual(self.plugin_shutdowns() +
                             self.app_shutdown_actions,
                             self.shutdown_actions)

    def test_early_exit(self):
        self.instantiate(LoopEarlyExit)
        self.diablo_run()
        self.assertTrue(self.diablo.loop_ran, 'App loop did not run.')
        self.assertListEqual(self.plugin_shutdowns() +
                             self.app_shutdown_actions,
                             self.shutdown_actions)

    def test_app_thread_exc(self):
        self.instantiate(LoopError)
        self.diablo.register_plugin(GoodPlugin)
        self.diablo_run()
        self.assertListEqual(self.plugin_shutdowns() +
                             self.app_shutdown_actions,
                             self.shutdown_actions)

    def test_app_startup_exc(self):
        self.instantiate(StartupError)
        self.diablo.register_plugin(GoodPlugin)
        self.diablo_run(Exception)
        self.assertListEqual(self.plugin_shutdowns(), self.shutdown_actions)

    def test_app_shutdown_exc(self):
        self.send_stop = True
        self.instantiate(ShutdownError)
        self.diablo.register_plugin(GoodPlugin)
        self.diablo_run()
        self.assertListEqual(self.plugin_shutdowns() +
                             self.app_shutdown_actions,
                             self.shutdown_actions)

    def test_plugin_init_error(self):
        self.instantiate(RegularLoop)
        self.assertRaises(Exception, self.diablo.register_plugin,
                                     InitErrorPlugin)

    def test_plugin_startup_error(self):
        self.instantiate(RegularLoop)
        self.diablo.register_plugin(GoodPlugin)
        self.diablo.register_plugin(StartErrorPlugin)
        self.diablo.register_plugin(GoodPlugin2)
        self.diablo_run(Exception)
        self.assertListEqual([self.diablo._plugins['good'].shutdown],
                             self.shutdown_actions)

    def test_plugin_shutdown_error(self):
        self.send_stop = True
        self.instantiate(RegularLoop)
        self.diablo.register_plugin(GoodPlugin)
        self.diablo.register_plugin(ShutdownErrorPlugin)
        self.diablo.register_plugin(GoodPlugin)
        self.diablo_run()
        self.assertTrue(self.diablo.loop_exited, 'App loop did not exit')
        self.assertListEqual(self.plugin_shutdowns() +
                             self.app_shutdown_actions,
                             self.shutdown_actions)

    def test_uptime_not_started(self):
        self.instantiate(RegularLoop)
        self.assertEqual(self.diablo.get_daemon_uptime(), 0.0)
        self.assertEqual(self.diablo.get_app_uptime(), 0.0)

    def test_uptime_started(self):
        self.send_stop = True
        self.instantiate(RegularLoop)
        self.diablo_run()
        self.assertGreater(self.diablo.get_daemon_uptime(), 0.0)
        self.assertGreater(self.diablo.get_app_uptime(), 0.0)


    def save_shutdown_actions(self):
        self.shutdown_actions = copy.copy(self.diablo._shutdown_actions)
        self.orig_do_shutdown()


class GoodPlugin(object):

    NAME = 'good'

    def __init__(self, *args, **kwargs):
        pass

    def startup(self):
        pass

    def shutdown(self):
        pass

    def should_start_app(self):
        return True


class GoodPlugin2(GoodPlugin):

    NAME = 'zgood2'


class InitErrorPlugin(object):

    NAME = 'init'

    def __init__(self, *args, **kwargs):
        raise Exception()

    def startup(self):
        pass

    def shutdown(self):
        pass


class StartErrorPlugin(object):

    NAME = 'start'

    def __init__(self, *args, **kwargs):
        pass

    def startup(self):
        raise Exception()

    def shutdown(self):
        pass


class ShutdownErrorPlugin(object):

    NAME = 'shutdown'

    def __init__(self, *args, **kwargs):
        pass

    def startup(self):
        pass

    def shutdown(self):
        raise Exception()

    def should_start_app(self):
        return True

class RegularLoop(shared.diablo.base.DiabloBase):

    @shared.diablo.base.app_thread
    def loop(self):
        while self.should_continue():
            time.sleep(0.3)
        self.loop_exited = True


class LoopEarlyExit(shared.diablo.base.DiabloBase):

    @shared.diablo.base.app_thread
    def loop(self):
        self.loop_ran = True


class LoopError(shared.diablo.base.DiabloBase):

    @shared.diablo.base.app_thread
    def loop(self):
        raise Exception()


class StartupError(RegularLoop):

    def app_startup(self):
        raise Exception()

    def loop(self):
        while self.should_continue():
            time.sleep(0.3)
        self.loop_exited = True


class ShutdownError(RegularLoop):

    def loop(self):
        while self.should_continue():
            time.sleep(0.3)
        self.loop_exited = True

    def app_shutdown(self):
        raise Exception()


class TestSupportMethods(unittest.TestCase):

    def setUp(self):
        self.diablo = BaseWithVersion({}, 'diablo', 'test', 0)
        self.diablo.register_plugin(StatusPlugin, 'arg1', arg2='arg2' )
        self.log = logging.getLogger()

    def test_plugin_extra_args(self):
        self.assertEqual(self.diablo._plugins['status'].arg0, 'arg1')
        self.assertEqual(self.diablo._plugins['status'].arg2, 'arg2')

    def test_get_status(self):
        keys = set(('app_name', 'node_id', 'version', 'hostname', 'pid',
                    'app_started', 'daemon_uptime', 'app_uptime', 'plugins',
                    'user', 'group', 'app'))
        status = self.diablo.get_status()
        self.assert_(keys.issubset(set(status.keys())))
        self.assertIsInstance(status['plugins']['status'], dict)

    def test_web_status(self):
        self.assertIsInstance(self.diablo.web_status('test'), str)

    def test_rpc_status(self):
        self.assertIsInstance(self.diablo.rpc_status(), dict)

    def my_stop_daemon(self, reason=None):
        self.stop_called = True

    def test_web_restart(self):

        self.diablo.stop_daemon = self.my_stop_daemon
        self.stop_called = False

        # Did not give confirm param
        self.diablo.web_restart({})
        self.assertFalse(self.stop_called)

        self.diablo.web_restart({'submit': ('Value',)})
        self.assertTrue(self.stop_called)

    def test_rpc_restart(self):

        self.diablo.stop_daemon = self.my_stop_daemon
        self.stop_called = False

        self.diablo.rpc_restart()
        self.assertTrue(self.stop_called)

    def patch_logging(self):
        self.addCleanup(setattr, shared.diablo.base.logging, 'getLogger',
                        shared.diablo.base.logging.getLogger)

        shared.diablo.base.logging.getLogger = self.my_getLogger

    def my_getLogger(self):
        self.start_log_level = self.log.level
        return self.log

    def test_change_log_level_no_param(self):
        self.patch_logging()
        ret = self.diablo.change_log_level({})
        self.assertIsInstance(ret, str)
        self.assertEqual(self.start_log_level, self.log.level)

    def test_change_log_level_bad_param(self):
        self.patch_logging()
        ret = self.diablo.change_log_level({'level': ('foo',)})
        self.assertIsInstance(ret, str)
        self.assertEqual(self.start_log_level, self.log.level)

    def test_change_log_level_warn_param(self):
        self.patch_logging()
        ret = self.diablo.change_log_level({'level': ('warn',)})
        self.assertIsInstance(ret, str)
        self.assertEqual(logging.WARN, self.log.level)
        self.log.setLevel(self.start_log_level)



class BaseWithVersion(shared.diablo.base.DiabloBase):

    VERSION = 'foo'


class StatusPlugin(object):

    NAME = 'status'

    def __init__(self, daemon_obj, conf, conf_section, *args, **kwargs):
        for i, arg in enumerate(args):
            setattr(self, 'arg' + str(i), arg)

        for k, v in kwargs.items():
            setattr(self, k, v)

    def get_status(self):
        return {}


if __name__ == '__main__':
    unittest.main()
