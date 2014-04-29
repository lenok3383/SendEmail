"""Unit tests for shared.conf.config module.

:Status: $Id: //prod/main/_is/shared/python/conf/test/test_config.py#10 $
:Authors: lromanov
"""

import os
import shutil
import stat
import tempfile
import unittest2 as unittest

import shared.conf
import shared.conf.config
import shared.conf.env

class TestConfig(unittest.TestCase):

    TEST_CONF_FILE1 = """
[cat0]
key0=eval(3600)
key1=eval(float(10.0))
key2=eval(float(-10.0))

[cat1]
key0=ironport
key1=san_bruno
key2=secapps"""

    TEST_CONF_FILE2 = """
[cat2]
key0=sbrs
key1=xbrs
key2=wbrs

[cat3]
key0=eval(900)
key1=eval(float(40.0))
key2=eval(float(-20.0))"""

    TEST_CONF_FILE3 = """
[cat4]
key0=eval(50)
key1=eval(float(4.5))

[cat5]
"""

    TEST_CONF_FILE4 = """
[cat6]
key0=['asdf', 'blah']
key1=eval(['asdf', 'blah'])

[cat7]
key0={'asdf': 'blah'}
key1=eval({'asdf': 'blah'})
"""

    def setUp(self):
        # Set the CONFROOT
        self.conf_dir = tempfile.mkdtemp(prefix='test_conf_')
        self.addCleanup(shutil.rmtree, self.conf_dir)

        old_confroot = os.environ.get('CONFROOT')
        if old_confroot is not None:
            self.addCleanup(os.environ.__setitem__, 'CONFROOT', old_confroot)
        else:
            self.addCleanup(os.environ.__delitem__, 'CONFROOT')
        os.environ['CONFROOT'] = self.conf_dir

        # Clear config cache. Do this at the end too to make sure we don't
        # screw anyone else up.
        shared.conf.config.clear_config_cache()
        self.addCleanup(shared.conf.config.clear_config_cache)

    def _write_config(self, filename, data):
        with open(self._get_config_path(filename), 'w') as conf_file:
            conf_file.write(data)

    def _get_config_path(self, filename):
        return os.path.join(shared.conf.env.get_conf_root(),
                            '%s.conf' % (filename,))

    # Tests valid and invalid filenames
    def test_get_config_filenames(self):
        with self.assertRaises(AssertionError):
            shared.conf.get_config('test_file1.conf')
        with self.assertRaises(AssertionError):
            shared.conf.get_config('/test_file1')
        shared.conf.get_config('test_file1')
        shared.conf.get_config('test_dir/test_file1')

    # Dict access tests with seed data
    def test_get_value_with_mode(self):
        self._write_config('test_file1', TestConfig.TEST_CONF_FILE1)
        self._write_config('test_file2', TestConfig.TEST_CONF_FILE2)

        conf = shared.conf.get_config('test_file1')
        with conf as c:
            self.assertEquals(c.get('cat0.key0'), 3600)
            self.assertEquals(c['cat0.key0'], 3600)
            self.assertEquals(c.get('some_key', 50), 50)

        conf2 = shared.conf.get_config('test_file2')
        with conf2 as c:
            self.assertEquals(c.get('cat2.key1'), 'xbrs')
            self.assertEquals(c.get('some_key', 50), 50)
            self.assertEquals(c['cat2.key1'], 'xbrs')

    def test_value_not_exist(self):
        self._write_config('test_file1', TestConfig.TEST_CONF_FILE1)
        with shared.conf.get_config('test_file1') as conf:
            with self.assertRaises(KeyError):
                c = conf['some_key']

    def test_evaluate_false(self):
        self._write_config('test_file4', TestConfig.TEST_CONF_FILE4)
        with shared.conf.get_config('test_file4') as conf:
            self.assertEquals(conf.get(key='cat6.key1',
                                       evaluate=False),
                              "eval(['asdf', 'blah'])")

    def test_get_value_solo_statement_mode(self):
        self._write_config('test_file1', TestConfig.TEST_CONF_FILE1)
        conf = shared.conf.get_config('test_file1')
        self.assertEquals(conf.get('cat0.key0'), 3600)
        self.assertEquals(conf['cat0.key0'], 3600)

    def test_changed_conf_file(self):
        self._write_config('test_file1', TestConfig.TEST_CONF_FILE1)
        conf = shared.conf.get_config('test_file1')
        self.assertEquals(conf.get('cat0.key0'), 3600)
        self.assertEquals(conf['cat0.key0'], 3600)

        # Change the file
        self._write_config('test_file1', TestConfig.TEST_CONF_FILE2)

        # Update the file's mtime to make it seem old
        path = self._get_config_path('test_file1')
        file_stat = os.stat(path)
        os.utime(path, (file_stat[stat.ST_ATIME], file_stat[stat.ST_MTIME] - 60))

        # Grab the same configuration again and make sure it was reloaded
        self.assertEquals(conf.get('cat2.key1'), 'xbrs')
        self.assertEquals(conf['cat2.key1'], 'xbrs')

    def test_set_clear_cache(self):
        """set_config sets config objects and clear_config_cache clears."""
        self._write_config('test_file1', TestConfig.TEST_CONF_FILE1)
        my_foo = object()
        shared.conf.config.set_config('foo', my_foo)

        conf1 = shared.conf.get_config('test_file1')

        self.assertEqual(len(shared.conf.config._config_cache), 2)
        shared.conf.config.clear_config_cache()
        self.assertEqual(len(shared.conf.config._config_cache), 0)

        # Previously set config is gone.
        f = shared.conf.get_config('foo')
        self.assertTrue(isinstance(f, shared.conf.config.Config))
        with self.assertRaises(shared.conf.config.ConfigFileMissingError):
            f.get('asdf')

        # Reading from file still works, but a new Config object is created.
        new_conf1 = shared.conf.get_config('test_file1')
        self.assertIsNot(new_conf1, conf1)

    def test_set_config(self):
        """Verifies set config can be fetched."""
        my_foo = object()
        shared.conf.config.set_config('foo', my_foo)
        self.assertIs(shared.conf.get_config('foo'), my_foo)

    def test_set_config_no_confroot(self):
        """ Verifies set config can be fetched without having CONFROOT set."""
        os.environ.pop('CONFROOT')
        self.addCleanup(os.environ.__setitem__, 'CONFROOT', self.conf_dir)
        my_foo = object()
        shared.conf.config.set_config('foo', my_foo)
        self.assertIs(shared.conf.get_config('foo'), my_foo)

    def test_conf_file_does_not_exist(self):
        conf = shared.conf.get_config('conf_not_here')
        self.assertRaises(
            shared.conf.config.ConfigFileMissingError, conf.get, 'some_key')

    def test_get_sections(self):
        self._write_config('test_file1', TestConfig.TEST_CONF_FILE1)
        conf = shared.conf.get_config('test_file1')
        ro_conf = conf.get_snapshot()
        sections = ro_conf.sections()
        self.assertEquals(sections, ['cat0', 'cat1'])

    def test_empty_section(self):
        self._write_config('test_file3', TestConfig.TEST_CONF_FILE3)
        conf = shared.conf.get_config('test_file3')
        ro_conf = conf.get_snapshot()
        sections = ro_conf.sections()
        self.assertTrue('cat5' in sections)

    def test_list(self):
        self._write_config('test_file4', TestConfig.TEST_CONF_FILE4)
        conf = shared.conf.get_config('test_file4')
        self.assertEquals(conf.get('cat6.key0'), ['asdf', 'blah'])
        self.assertEquals(conf.get('cat6.key1'), ['asdf', 'blah'])

    def test_dict(self):
        self._write_config('test_file4', TestConfig.TEST_CONF_FILE4)
        conf = shared.conf.get_config('test_file4')
        self.assertEquals(conf.get('cat7.key0'), {'asdf': 'blah'})
        self.assertEquals(conf.get('cat7.key1'), {'asdf': 'blah'})

    def test_config_from_dict(self):
        my_config = shared.conf.config.ConfigFromDict(
            {'my.foo': 'bar', 'yours.blah': 'eval(int(3600))', 'my.quux': 90})
        shared.conf.config.set_config('foo', my_config)

        self.assertIs(shared.conf.get_config('foo'), my_config)

        self.assertEqual(my_config['my.foo'], 'bar')
        self.assertEqual(my_config.get('my.foo'), 'bar')
        self.assertEqual(my_config.get('my.nope'), None)
        self.assertEqual(my_config.get('my.nope', 42), 42)
        self.assertEqual(my_config.get('yours.blah'), 3600)
        self.assertEqual(my_config.get('my.quux'), 90)

        snapshot = my_config.get_snapshot()
        self.assertEqual(snapshot['my.foo'], 'bar')
        self.assertEqual(snapshot.get('my.foo'), 'bar')
        self.assertEqual(snapshot.get('my.nope'), None)
        self.assertEqual(snapshot.get('my.nope', 42), 42)
        self.assertEqual(snapshot.get('yours.blah'), 3600)
        self.assertEqual(snapshot.get('yours.blah', evaluate=False),
                         'eval(int(3600))')
        self.assertEqual(snapshot.get('my.quux'), 90)
        # Not supported/undefined: snapshot.get('my.quux', evaluate=False)

        self.assertItemsEqual(snapshot.sections(), ['my', 'yours'])
        self.assertTrue(isinstance(snapshot.sections(), list))

        with my_config as c:
            self.assertEqual(c.get('my.foo'), 'bar')

if __name__ == '__main__':
    # Run unit tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestConfig)
    unittest.TextTestRunner(verbosity=2).run(suite)
