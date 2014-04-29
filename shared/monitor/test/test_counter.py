import json
import threading
import time
import unittest2 as unittest

import shared.monitor.counter

class ThreadSafeCounterTestCase(unittest.TestCase):

    def test_thread_safety(self):

        def increase_1000(counter):
            for i in xrange(1000):
                counter.increase()

        def decrease_1000(counter):
            for i in xrange(1000):
                counter.decrease()

        counter = shared.monitor.counter.ThreadSafeCounter()
        threads = [ threading.Thread(target=increase_1000, args=[counter]) for x in xrange(100) ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(counter.get_value(), 100000)
        threads = [ threading.Thread(target=decrease_1000, args=[counter]) for x in xrange(100) ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(counter.get_value(), 0)

    def test_base_functionality(self):

        counter = shared.monitor.counter.ThreadSafeCounter()
        counter.set_value(100)
        self.assertEqual(counter.get_value(), 100)
        counter.reset()
        self.assertEqual(counter.get_value(), 0)
        counter.increase(10)
        self.assertEqual(counter.get_value(), 10)
        counter.decrease(1)
        self.assertEqual(counter.get_value(), 9)


class MemoryCounterStorage(shared.monitor.counter.BaseCounterStorage):

    values = {}

    @classmethod
    def store(cls, key, value, name):
        cls.values[key] = value

shared.monitor.counter.BaseCounter.set_storage(MemoryCounterStorage())

class StaticCounterTestCase(unittest.TestCase):

    def setUp(self):
        # reset the global dict between tests
        shared.monitor.counter._G_COUNTERS = {}

    def test_base_functionality(self):

        counter = shared.monitor.counter.StaticCounter('unittest:base',
                                               reset_value=0,
                                               min_interval=.1,
                                               max_history_depth=3)
        self.assertEqual(counter.key, 'unittest:base:st')
        self.assertEqual(counter.value, 0)
        time.sleep(.11)
        counter.set(100)
        self.assertEqual(counter.value, 100)
        self.assertEqual(len(counter._history), 1)
        self.assertEqual(counter._history[-1][1], 0)
        counter.flush()
        self.assertEqual(MemoryCounterStorage.values[counter.key], 100)
        counter.set(200)
        # not enough time has elapsed to push the value to history
        self.assertEqual(len(counter._history), 1)
        self.assertEqual(counter._history[-1][1], 0)
        time.sleep(.11)
        counter.set(300)
        # now we push the last value in (200)
        self.assertEqual(len(counter._history), 2)
        self.assertEqual(counter._history[-1][1], 200)
        for i in xrange(3):
            time.sleep(.11)
            counter.set(300)
        self.assertEqual(len(counter._history), 3)
        self.assertTrue('</a>' in counter.html())
        j = counter.json()
        d = json.loads(j)
        self.assertEquals(d['type'], 'StaticCounter')
        self.assertEquals(d['name'], 'unittest:base')
        self.assertEquals(d['key'], 'unittest:base:st')
        self.assertEquals(d['value'], 300)


    def test_timstamp_functionality(self):

        counter = shared.monitor.counter.Timestamp('unittest:base',
                                               reset_value=0,
                                               min_interval=.1,
                                               max_history_depth=3,
                                               timestamp=True)
        self.assertEqual(counter.key, 'unittest:base:ts')
        counter.set(time.time())
        self.assertFalse('</a>' in counter.html())

    def test_static_convenience_functions(self):
        shared.monitor.counter.init_static_counter('unittest:static', min_interval=.1)
        c1 = shared.monitor.counter.get_counter('unittest:static')
        self.assertEquals(c1.key, 'unittest:static:st')
        c2 = shared.monitor.counter.get_counter_from_key('unittest:static:st')
        self.assertEquals(c1, c2)
        shared.monitor.counter.set_value('unittest:static', 10)
        self.assertEquals(shared.monitor.counter.get_value('unittest:static'), 10)
        shared.monitor.counter.reset_all_counters()
        self.assertEquals(c1.value, 0)
        self.assertEquals(c2.value, 0)
        cs = shared.monitor.counter.get_counters_for_section('unittest')
        self.assertEquals(cs[0], c1)
        self.assertSequenceEqual(cs, shared.monitor.counter.get_all_counters())

class DynamicCounterTestCase(unittest.TestCase):

    def setUp(self):
        # reset the global dict between tests
        shared.monitor.counter._G_COUNTERS = {}

    def test_base_functionality(self):
        counter = shared.monitor.counter.DynamicCounter('unittest:base',
                                                reset_value=0,
                                                min_interval=.1,
                                                max_history_depth=3,
                                                min_rate_interval=1)
        self.assertEqual(counter.key, 'unittest:base:dyn')
        self.assertEqual(counter.rate_key, 'unittest:base:r0')
        self.assertEqual(counter.value, 0)
        time.sleep(.11)
        counter.set(100)
        self.assertEqual(counter.value, 100)
        self.assertEqual(len(counter._history), 1)
        self.assertEqual(counter._history[-1][1], 0)
        counter.flush()
        self.assertEqual(MemoryCounterStorage.values[counter.key], 100)
        counter.set(200)
        self.assertEqual(len(counter._history), 1)
        self.assertEqual(counter._history[-1][1], 0)
        time.sleep(.11)
        counter.set(300)
        self.assertEqual(len(counter._history), 2)
        self.assertEqual(counter._history[-1][1], 200)
        for i in xrange(3):
            time.sleep(.11)
            counter.set(300)
        self.assertEqual(len(counter._history), 3)
        self.assertTrue('</a>' in counter.html())
        j = counter.json()
        d = json.loads(j)
        self.assertEquals(d['type'], 'DynamicCounter')
        self.assertEquals(d['name'], 'unittest:base')
        self.assertEquals(d['key'], 'unittest:base:dyn')
        self.assertEquals(d['rate_key'], 'unittest:base:r0')
        self.assertEquals(d['value'], 300)

    def test_rate_functionality(self):
        counter = shared.monitor.counter.DynamicCounter('unittest:rate',
                                                reset_value=0,
                                                min_interval=.1,
                                                max_history_depth=10)
        for i in xrange(3):
            counter.increment()
            time.sleep(.101)
        self.assertAlmostEqual(counter.rate[0], 10, delta=2)
        self.assertAlmostEqual(counter.rate[1], 10, delta=2)
        counter.increment(1)
        counter.increment(1)
        counter.increment(1)
        counter.increment(3)
        time.sleep(.101)
        # force an update
        counter._update_history()
        # we've increased a total of 9 in 4 seconds
        self.assertAlmostEqual(counter.rate[1], 22.5, delta=2)
        # we've increased 6 in 1 second
        self.assertAlmostEqual(counter.rate[0], 60, delta=2)

    def test_rate_functionality_no_history(self):
        counter = shared.monitor.counter.DynamicCounter('unittest:no_history',
                                                reset_value=0,
                                                min_interval=0,
                                                max_history_depth=10)
        for i in xrange(3):
            counter.increment()
            time.sleep(.101)
        self.assertAlmostEqual(counter.rate[0], 10, delta=2)
        self.assertAlmostEqual(counter.rate[1], 10, delta=2)
        counter.increment(1)
        counter.increment(1)
        counter.increment(1)
        counter.increment(3)
        time.sleep(.101)
        # force an update
        counter._update_history()
        # we've increased a total of 9 in 4 seconds
        self.assertAlmostEqual(counter.rate[1], 22.5, delta=2)
        # we've increased 6 in 1 second
        self.assertAlmostEqual(counter.rate[0], 22.5, delta=2)

    def test_dynamic_convenience_functions(self):
        shared.monitor.counter.init_dynamic_counter('unittest:dynamic', min_interval=.1)
        c1 = shared.monitor.counter.get_counter('unittest:dynamic')
        self.assertEquals(c1.key, 'unittest:dynamic:dyn')
        self.assertEquals(c1.rate_key, 'unittest:dynamic:r0')
        c2 = shared.monitor.counter.get_counter_from_key('unittest:dynamic:dyn')
        self.assertEquals(c1, c2)
        shared.monitor.counter.increase('unittest:dynamic', 10)
        shared.monitor.counter.decrease('unittest:dynamic', 8)
        self.assertEquals(shared.monitor.counter.get_value('unittest:dynamic'), 2)
        shared.monitor.counter.reset_all_counters()
        self.assertEquals(c1.value, 0)
        self.assertEquals(c2.value, 0)
        cs = shared.monitor.counter.get_counters_for_section('unittest')
        self.assertEquals(cs[0], c1)
        self.assertSequenceEqual(cs, shared.monitor.counter.get_all_counters())

if __name__ == "__main__":
    thread_suite = unittest.TestLoader().loadTestsFromTestCase(ThreadSafeCounterTestCase)
    static_suite = unittest.TestLoader().loadTestsFromTestCase(StaticCounterTestCase)
    dynamic_suite = unittest.TestLoader().loadTestsFromTestCase(DynamicCounterTestCase)
    suite = unittest.TestSuite((thread_suite, static_suite, dynamic_suite))
    unittest.TextTestRunner(verbosity=2).run(suite)
