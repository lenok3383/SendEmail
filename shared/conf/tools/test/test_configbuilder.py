#!/usr/bin/env python
"""Unittests for configbuilder module.

:Status: $Id: //prod/main/_is/shared/python/conf/tools/test/test_configbuilder.py#8 $
:Authors: rbodnarc
"""

import os
import shutil
import tempfile
import unittest2 as unittest
import warnings

import shared.testing.case
from shared.conf.tools import configbuilder

_CONFIGVARS_NEW = """
[param1]
help=Parameter 1
default=Aa
validate_re=.+

[param2]
help=Parameter 2
type=int

[param3]
default=True
type=bool

[param4]
default=1.0
type=float

[param5]
type=list

[param6]
type=dict

[param7]
type=str
default=
"""

_CONFIGVARS_PURE = """
[file1.section1.param1]
help=Parameter 1
default=Aa
validate_re=.+

[file1.section2.param2]
help=Parameter 2
type=int
default=123

[file2.section1.param1]
type=list
default=[1,2,3]
"""

_CONFIGVARS_OLD = """
[param1]
prompt=Parameter 1
default=Aa
validate_re=.+
order=10

[param2]
prompt=Parameter 2

[param3]
prompt=Parameter 3
validate_re=.+
default=eval([1,2])

[param7]
default=
"""

_CONFIGFILES = """
template, file
"""

TEMPLATE_OLD_STYLE = """Parameter 1 %(param1)s.
Some format %s %t %Y $$ $$ $ $
Unknown parameter %(ababa)s
Empty value "%(param7)s"
"""

TEMPLATE = """Parameter 1 $param1.
Some format %s %t %Y $$ $$ $ $
Unknown parameter $ababa
Empty value "$param7"
"""

BUILT_TEMPLATE_OLD_STYLE = """Parameter 1 Bb.
Some format %s %t %Y $$ $$ $ $
Unknown parameter %(ababa)s
Empty value ""
"""

BUILT_TEMPLATE = """Parameter 1 Bb.
Some format %s %t %Y $ $ $ $
Unknown parameter $ababa
Empty value ""
"""

FILE1_CONF = """[section1]
param1 = Aa

[section2]
param2 = eval(123)

"""

SAVEDVARS = """param1=Bb
param2=123
param3=True
param4=2.222
param5=[1,2,3]
param6={"a" : "A"}
param7=
"""

SAVEDVARS_PURE = """file1.section1.param1=Aa
file1.section2.param2=123
file2.section1.param1=[1,2,3]
"""

# For testing how empty values are handled.
_CONFIGVARS_BLANK = """
[blank_default]
default=

[blank_invalid]
validate_re=.+

[blank_ok_1]

[blank_ok_2]
"""

SAVEDVARS_BLANK = """
blank_invalid=
blank_ok_1=
"""

TEMPLATE_BLANK = """
default=${blank_default}
ok_1=${blank_ok_1}
ok_2=${blank_ok_2}
invalid=${blank_invalid}
"""

BUILT_TEMPLATE_BLANK = """
default=
ok_1=
ok_2=foo
invalid=hello
"""

class TestParseVariables(shared.testing.case.TestCase):

    def setUp(self):
        self.tmp = self.get_temp_dir()

    def _write_file(self, name, data):
        path = os.path.join(self.tmp, name)
        with open(path, 'w') as fh:
            fh.write(data)
        return path

    def test_old_and_new(self):
        """Verify old and new configvars syntax are equivalent."""
        new = self._write_file('_configvars-new.conf', _CONFIGVARS_NEW)
        old = self._write_file('_configvars-old.conf', _CONFIGVARS_OLD)
        new_vars = configbuilder.parse_configvars_conf(new)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            old_vars = configbuilder.parse_configvars_conf(old)

            # The first variable of each should be equivalent.
            self.assertEqual(old_vars[0], new_vars[0])

        # order=10 should lead to a DeprecationWarning
        self.assertEqual(len(w), 1)
        self.assertEqual(w[0].category, DeprecationWarning)
        self.assertIn('order', str(w[0].message))

    def test_unknown_options(self):
        """Warn (possible typo) when unknown options in _configvars.conf."""
        configvars = self._write_file('_configvars.conf', '[foo]\nbar=baz\n')

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            configbuilder.parse_configvars_conf(configvars)

        # order=10 should lead to a DeprecationWarning
        self.assertEqual(len(w), 1)
        self.assertIn('bar', str(w[0].message))
        self.assertIn('unknown', str(w[0].message).lower())

class TestConfigBuilder(unittest.TestCase):

    def setUp(self):
        self.confroot = tempfile.mkdtemp()
        self.savedir = tempfile.mkdtemp()
        self.prompt_idx = 0
        self.prompts = list()
        self.user_responses = list()
        self.savedfile = os.path.join(self.savedir,
                                      self.confroot.replace(os.path.sep, '_'))

        self.original_raw_input = configbuilder._raw_input
        configbuilder._raw_input = self._raw_input

    def tearDown(self):
        shutil.rmtree(self.confroot, ignore_errors=True)
        shutil.rmtree(self.savedir, ignore_errors=True)

        configbuilder._raw_input = self.original_raw_input

    def _raw_input(self, prompt, default_value=None):
        try:
            self.assertEquals(prompt, self.prompts[self.prompt_idx])
            response = self.user_responses[self.prompt_idx]
        except IndexError:
            self.fail('Unexpected prompt: %s' % (prompt,))

        self.prompt_idx += 1
        return response

    def test_configbuilder_new_configvars(self):
        """Generates config with cache file specifying all args, no prompts."""
        with open(os.path.join(self.confroot,
                               '_configvars.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGVARS_NEW)

        with open(os.path.join(self.confroot,
                               '_configfiles.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGFILES)

        with open(os.path.join(self.confroot, 'template'), 'w') as fhandle:
            fhandle.write(TEMPLATE)

        with open(self.savedfile, 'w') as fhandle:
            fhandle.write(SAVEDVARS)

        configbuilder.do_configbuilder_build_files(confroot=self.confroot,
                                                   savedir=self.savedir)

        self.assertTrue(self.prompt_idx == 0)

        with open(os.path.join(self.confroot, 'file'), 'r') as fhandle:
            self.assertEqual(fhandle.read(), BUILT_TEMPLATE)

        # No changes to saved values
        with open(self.savedfile, 'r') as fhandle:
            self.assertEqual(fhandle.read(), SAVEDVARS)

    def test_configbuilder_new_configvars_force(self):
        """Prompts for all values with force option."""
        with open(os.path.join(self.confroot,
                               '_configvars.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGVARS_NEW)

        with open(os.path.join(self.confroot,
                               '_configfiles.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGFILES)

        with open(os.path.join(self.confroot, 'template'), 'w') as fhandle:
            fhandle.write(TEMPLATE)

        with open(self.savedfile, 'w') as fhandle:
            fhandle.write(SAVEDVARS)

        # Next interaction with user is expected:
        self.prompts = ['1. Parameter 1 (str) [param1] = [Bb]:',
                        '2. Parameter 2 (int) [param2] = [123]:',
                        '3. param3 (bool) [param3] = [True]:',
                        '4. param4 (float) [param4] = [2.222]:',
                        '5. param5 (list) [param5] = [[1,2,3]]:',
                        '6. param6 (dict) [param6] = [{"a" : "A"}]:',
                        '7. param7 (str) [param7] = []:'
                        ]

        self.user_responses = [''] * len(self.prompts)

        configbuilder.do_configbuilder_build_files(confroot=self.confroot,
                                                   savedir=self.savedir,
                                                   force=True)

        self.assertEqual(self.prompt_idx, len(self.user_responses))

        with open(os.path.join(self.confroot, 'file'), 'r') as fhandle:
            self.assertTrue(fhandle.read(), BUILT_TEMPLATE)

        with open(self.savedfile, 'r') as fhandle:
            self.assertEqual(fhandle.read(), SAVEDVARS)

    def test_configbuilder_input_validation(self):
        """Validates input provided at prompts."""
        with open(os.path.join(self.confroot,
                               '_configvars.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGVARS_NEW)

        with open(os.path.join(self.confroot,
                               '_configfiles.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGFILES)

        with open(os.path.join(self.confroot, 'template'), 'w') as fhandle:
            fhandle.write(TEMPLATE)

        with open(self.savedfile, 'w') as fhandle:
            fhandle.write(SAVEDVARS)

        # Next interaction with user is expected:
        self.prompts = ['1. Parameter 1 (str) [param1] = [Bb]:',
                        '2. Parameter 2 (int) [param2] = [123]:',
                        '2. Parameter 2 (int) [param2] = [123]:',
                        '2. Parameter 2 (int) [param2] = [123]:',
                        '3. param3 (bool) [param3] = [True]:',
                        '3. param3 (bool) [param3] = [True]:',
                        '3. param3 (bool) [param3] = [True]:',
                        '4. param4 (float) [param4] = [2.222]:',
                        '4. param4 (float) [param4] = [2.222]:',
                        '5. param5 (list) [param5] = [[1,2,3]]:',
                        '5. param5 (list) [param5] = [[1,2,3]]:',
                        '5. param5 (list) [param5] = [[1,2,3]]:',
                        '6. param6 (dict) [param6] = [{"a" : "A"}]:',
                        '6. param6 (dict) [param6] = [{"a" : "A"}]:',
                        '6. param6 (dict) [param6] = [{"a" : "A"}]:',
                        '7. param7 (str) [param7] = []:']
        self.user_responses = ['eval("")',
                               'qwe',
                               '[123]',
                               '123',
                               'AFalse',
                               'true',
                               '',
                               '12fgsd',
                               '1',
                               '123',
                               '1, 2, 3',
                               '[1, 2]',
                               '[]',
                               '123',
                               '',
                               '']

        self.assertEqual(len(self.prompts), len(self.user_responses))

        configbuilder.do_configbuilder_build_files(confroot=self.confroot,
                                                   savedir=self.savedir,
                                                   force=True)

        self.assertEqual(self.prompt_idx, len(self.user_responses))

        with open(os.path.join(self.confroot, 'file'), 'r') as fhandle:
            self.assertTrue(fhandle.read(), BUILT_TEMPLATE)

        # Saved values were changed
        with open(self.savedfile, 'r') as fhandle:
            self.assertNotEqual(fhandle.read(), SAVEDVARS)

    def test_configbuilder_no_savedvars(self):
        """Verify values without defaults are prompted when no cache exists."""
        with open(os.path.join(self.confroot,
                               '_configvars.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGVARS_NEW)

        with open(os.path.join(self.confroot,
                               '_configfiles.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGFILES)

        with open(os.path.join(self.confroot, 'template'), 'w') as fhandle:
            fhandle.write(TEMPLATE)

        # Next interaction with user is expected:
        self.prompts = ['2. Parameter 2 (int) [param2]:',
                        '2. Parameter 2 (int) [param2]:',
                        '5. param5 (list) [param5]:',
                        '5. param5 (list) [param5]:',
                        '6. param6 (dict) [param6]:',
                        '6. param6 (dict) [param6]:']
        self.user_responses = ['',
                               '42',
                               '',
                               '[123]',
                               '',
                               '{1:1}'
                               ]
        self.assertEqual(len(self.prompts), len(self.user_responses))

        configbuilder.do_configbuilder_build_files(confroot=self.confroot,
                                                   savedir=self.savedir)

        self.assertEqual(self.prompt_idx, len(self.user_responses))

        with open(os.path.join(self.confroot, 'file'), 'r') as fhandle:
            self.assertTrue(fhandle.read(), BUILT_TEMPLATE)

        # Saved values were changed
        with open(self.savedfile, 'r') as fhandle:
            self.assertNotEqual(fhandle.read(), SAVEDVARS)

    def test_configbuilder_pure_config(self):
        """Defined variables of form [foo.bar.baz] lead to a foo.conf file."""
        with open(os.path.join(self.confroot,
                               '_configvars.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGVARS_PURE)

        # Expect no interaction with user
        self.user_responses = dict()

        configbuilder.do_configbuilder_build_files(
            confroot=self.confroot, savedir=self.savedir)

        self.assertTrue(self.prompt_idx == 0)

        files = os.listdir(self.confroot)
        self.assertTrue('file1.conf' in files)
        self.assertTrue('file2.conf' in files)

        with open(os.path.join(self.confroot, 'file1.conf'), 'r') as fhandle:
            self.assertEqual(fhandle.read(), FILE1_CONF)

        # No changes to saved values
        with open(self.savedfile, 'r') as fhandle:
            self.assertEqual(fhandle.read(), SAVEDVARS_PURE)


    def test_configbuilder_old_style_template(self):
        """Tests backwards compatibility with old style templates."""
        with open(os.path.join(self.confroot,
                               '_configvars.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGVARS_NEW)

        with open(os.path.join(self.confroot,
                               '_configfiles.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGFILES)

        with open(os.path.join(self.confroot, 'template'), 'w') as fhandle:
            fhandle.write(TEMPLATE_OLD_STYLE)

        with open(self.savedfile, 'w') as fhandle:
            fhandle.write(SAVEDVARS)

        # Expect no interaction with user
        self.user_responses = dict()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')

            # We're using an old style template, we should get a warning.
            configbuilder.do_configbuilder_build_files(
                confroot=self.confroot, savedir=self.savedir)

        self.assertEqual(len(w), 1)
        self.assertEqual(w[0].category, DeprecationWarning)

        self.assertTrue(self.prompt_idx == 0)

        with open(os.path.join(self.confroot, 'file'), 'r') as fhandle:
            self.assertEqual(fhandle.read(), BUILT_TEMPLATE_OLD_STYLE)

        # No changes to saved values
        with open(self.savedfile, 'r') as fhandle:
            self.assertEqual(fhandle.read(), SAVEDVARS)

    def test_configbuilder_report(self):
        """Tests configbuilder can produce a report."""
        with open(os.path.join(self.confroot,
                               '_configvars.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGVARS_NEW)

        with open(self.savedfile, 'w') as fhandle:
            fhandle.write(SAVEDVARS)

        report = configbuilder.do_configbuilder_report(
            confroot=self.confroot, savedir=self.savedir)

        self.assertNotEquals(report, '')

    def test_handle_empty_vals(self):
        """Empty values can be confirmed but not specified at a prompt."""
        with open(os.path.join(self.confroot,
                               '_configvars.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGVARS_BLANK)

        with open(os.path.join(self.confroot,
                               '_configfiles.conf'), 'w') as fhandle:
            fhandle.write(_CONFIGFILES)

        with open(self.savedfile, 'w') as fhandle:
            fhandle.write(SAVEDVARS_BLANK)

        with open(os.path.join(self.confroot, 'template'), 'w') as fhandle:
            fhandle.write(TEMPLATE_BLANK)

        # Next interaction with user is expected:
        self.prompts = ['1. blank_default (str) [blank_default] = []:',
                        '2. blank_invalid (str) [blank_invalid] = []:',
                        '2. blank_invalid (str) [blank_invalid] = []:',
                        '3. blank_ok_1 (str) [blank_ok_1] = []:',
                        '4. blank_ok_2 (str) [blank_ok_2]:',
                        '4. blank_ok_2 (str) [blank_ok_2]:']
        self.user_responses = ['',
                               '',
                               'foo',
                               '',
                               '',
                               'hello']
        self.assertEqual(len(self.prompts), len(self.user_responses))

        configbuilder.do_configbuilder_build_files(
            confroot=self.confroot, savedir=self.savedir, force=True)

        self.assertEqual(self.prompt_idx, len(self.user_responses))

        with open(os.path.join(self.confroot, 'file'), 'r') as fhandle:
            self.assertTrue(fhandle.read(), BUILT_TEMPLATE_BLANK)

        # Saved values were changed
        with open(self.savedfile, 'r') as fhandle:
            self.assertNotEqual(fhandle.read(), SAVEDVARS_BLANK)

if __name__ == '__main__':
    unittest.main()
