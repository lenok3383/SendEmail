#!/usr/bin/env python
"""Unittests for threadutils module.

:Status: $Id: //prod/main/_is/shared/python/util/test/test_threadutils.py#8 $
:Authors: bejung, rbodnarc
"""

import operator
import time
import unittest2 as unittest

import shared.testing.case
import shared.util.threadutils


class SimpleTaskDispatcherTestCase(shared.testing.case.TestCase):

    """Test all public components in SimpleTaskDispatcher"""

    def setUp(self):
        self.do_thread_check()

    def tearDown(self):
        pass

    def _append_result_handler(self, results, arg, time_to_sleep=0.01):
        time.sleep(time_to_sleep)
        results.append(arg)

    def test_base_functionality(self):
        std = shared.util.threadutils.SimpleTaskDispatcher(
            max_num_workers=1, max_worker_idle_period=0.1)
        try:
            std.register_handler('append_result', self._append_result_handler)
            self.assertEquals(std.lookup_handler('append_result'),
                              self._append_result_handler)
            self.assertEquals(std.queue_size(), 0)
            self.assertEquals(std.num_worker_threads(), 0)
            self.assertEquals(std.num_idle_worker_threads(), 0)

            results = []
            std.add_task('append_result', results, 1)
            with self.assertRaises(
                    shared.util.threadutils.UnknownTaskException):
                std.add_task('no_handler', 'data')
            self.assertTrue(std.wait_until_done())
            self.assertEquals(len(results), 1)
            self.assertEquals(results[0], 1)
            # allow time for the thread to clean itself up
            time.sleep(0.3)
            self.assertEquals(std.num_worker_threads(), 0)
        finally:
            std.stop_all()

    def test_base_functionality_2(self):
        std = shared.util.threadutils.SimpleTaskDispatcher(
            max_num_workers=2, max_worker_idle_period=0.1)
        try:
            self.assertEquals(std.queue_size(), 0)
            self.assertEquals(std.num_worker_threads(), 0)
            self.assertEquals(std.num_idle_worker_threads(), 0)

            results = []
            std.add_task(self._append_result_handler, results, 1)
            self.assertTrue(std.wait_until_done())
            self.assertEquals(len(results), 1)
            self.assertEquals(results[0], 1)
        finally:
            std.stop_all()

    def test_concurrency(self):
        std = shared.util.threadutils.SimpleTaskDispatcher(
            max_num_workers=8, max_worker_idle_period=0.1)
        try:
            std.register_handler('append_result', self._append_result_handler)
            results = []
            for i in xrange(8):
                std.add_task('append_result', results, i)
            self.assertTrue(std.wait_until_done())
            self.assertItemsEqual(results, range(8))
        finally:
            std.stop_all()

    def test_limits(self):
        std = shared.util.threadutils.SimpleTaskDispatcher(
            max_num_workers=3, max_worker_idle_period=0.1, max_queue_size=8)
        try:
            std.register_handler('append_result', self._append_result_handler)
            results = []
            for i in xrange(8):
                std.add_task('append_result', results, i, time_to_sleep=0.2)
            with self.assertRaises(
                    shared.util.threadutils.TaskQueueIsFullError):
                std.add_task_no_wait('append_result', results, 8)
            with self.assertRaises(
                    shared.util.threadutils.TaskQueueIsFullError):
                std.add_task_no_wait(self._append_result_handler, results, 8)
            std.add_task('append_result', results, 8)
            time.sleep(0.05)
            self.assertLessEqual(std.num_worker_threads(), 3)
            self.assertTrue(std.wait_until_done())
            self.assertItemsEqual(results, range(9))
        finally:
            std.stop_all()

    def test_wait_until_done(self):
        # Tests that wait_until_done() properly returns False if it times out
        # while waiting for the queue to empty.
        std = shared.util.threadutils.SimpleTaskDispatcher(
            max_num_workers=2, max_worker_idle_period=0.1)
        try:
            results = []
            for i in xrange(5):
                std.add_task(self._append_result_handler, results, 1,
                             time_to_sleep=0.5)
            self.assertFalse(std.wait_until_done(1.0))
        finally:
            std.stop_all()

    def test_wait_until_done_2(self):
        # Tests that wait_until_done() properly returns False if it times out
        # while waiting for a worker to finish a task.
        std = shared.util.threadutils.SimpleTaskDispatcher(
            max_num_workers=2, max_worker_idle_period=0.1)
        try:
            results = []
            std.add_task(self._append_result_handler, results, 1,
                         time_to_sleep=2.0)
            self.assertFalse(std.wait_until_done(1.0))
        finally:
            std.stop_all()


class SimpleJobExecutorTestCase(shared.testing.case.TestCase):

    """Just a black box test, since there are no public methods, and
    SimpleJobExecuter is just a wrapper around SimpleTaskDispatcher.
    """

    def setUp(self):
        self.do_thread_check()

    def test_normal_executor(self):

        def task_executor(l):
            return reduce(operator.add, l)

        answer = sum(xrange(100000))
        self.assertEqual(self._test_executor(task_executor, 100000), answer)

    def test_slow_executor(self):

        def slow_task_executor(l):
            time.sleep(1.5)
            return reduce(operator.add, l)

        with self.assertRaises(shared.util.threadutils.TimeoutException):
            self._test_executor(slow_task_executor, 10, timeout=0.5)

    def _test_executor(self, executor, range_size, timeout=None):

        def task_slicer(l):
            tasks = []
            slice_size = 10
            start_pos = 0
            next_pos = 0
            size = len(l)
            while next_pos < size:
                start_pos = next_pos
                next_pos += slice_size
                if next_pos > size:
                    next_pos = size
                task = l[start_pos:next_pos]
                tasks.append(((task,), {}))
            return tasks

        def results_consolidator(d):
            return reduce(operator.add, d.values())

        std = shared.util.threadutils.SimpleTaskDispatcher(max_num_workers=8)
        try:
            sje = shared.util.threadutils.SimpleJobExecutor(
                std, executor, task_slicer, results_consolidator,
                timeout=timeout)
            return sje(range(range_size))
        finally:
            std.stop_all()


class TimeoutFuncTestCase(shared.testing.case.TestCase):

    """Tests for timeout_func function.
    """

    def setUp(self):
        self.do_thread_check()

    def test_normal(self):
        def _func():
            return 1

        res = shared.util.threadutils.timeout_func(_func)
        answer = 1
        self.assertEqual(res, answer)

    def test_normal_args(self):
        def _func(a, b):
            return a + b

        res = shared.util.threadutils.timeout_func(_func, args=(2, 3))
        answer = 5
        self.assertEqual(res, answer)

    def test_normal_kwargs(self):
        def _func(a=0, b=0):
            return a * b

        res = shared.util.threadutils.timeout_func(_func, kwargs={'a': 3,
                                                                  'b': 4})
        answer = 12
        self.assertEqual(res, answer)

    def test_exception(self):
        def _func():
            raise ValueError('test')

        with self.assertRaises(ValueError):
            shared.util.threadutils.timeout_func(_func)

    def test_timeout(self):
        def _func():
            time.sleep(0.2)
            return 1

        with self.assertRaises(shared.util.threadutils.TimeoutException):
            shared.util.threadutils.timeout_func(_func, timeout=0.1)

        time.sleep(0.2)

    def test_exception_after_timeout(self):
        def _func():
            time.sleep(0.2)
            raise ValueError('test')

        with self.assertRaises(shared.util.threadutils.TimeoutException):
            shared.util.threadutils.timeout_func(_func, timeout=0.1)

        time.sleep(0.2)

if __name__ == '__main__':
    unittest.main()
