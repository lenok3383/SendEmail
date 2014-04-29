#!/usr/bin/env python
"""Unit test for ipconf_gen.

:Status: $Id: //prod/main/_is/shared/python/conf/tools/test/test_ipconf_gen.py#3 $
:Author: bwhitela
"""

import logging
import os
import pwd
import re
import socket
import tempfile
import unittest2 as unittest

from shared.conf.tools import configbuilder
from shared.conf.tools import ipconf
from shared.conf.tools import ipconf_gen


# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)


# Content for test files.
FILE_WITH_MACRO = """This is the first line.
This is the second.

This is the macro line: $%s$
""" % ('Id: macro_text ',)

FILE_WITHOUT_MACRO = """This is the first line.
This is the second.

This is the non-macro line.
"""


CONFIGVARS = [{'name': 'one', 'default': '1', 'var_help': 'Help statement 1',
               'validate_regex': '\d+', 'var_type': 'int'},
              {'name': 'two', 'default': None, 'var_help': 'Help statement 2'},
              {'name': 'three', 'default': 'three', 'var_help': 'Help statement 3',
               'validate_regex': '[a-zA-Z]+', 'var_type': 'str'}]

EXPECTED = """# Help statement 1
;one=1

# NOTE: No default/cached value specified!
# Help statement 2
two=

# Help statement 3
;three=three"""

CORRECT_CACHED = {'one': '1', 'two': '2', 'three': 'three'}

EXPECTED_CACHED = """# Help statement 1
one=1

# Help statement 2
two=2

# Help statement 3
three=three"""

BAD_CACHED = {'one': 'one', 'three': 'three'}

EXPECTED_BAD_CACHED = """#
# 1 ERROR found when upgrading cache file!
#

# ERROR: Failed validation! type: int, regex: \d+
# Help statement 1
one=one

# NOTE: No default/cached value specified!
# Help statement 2
two=

# Help statement 3
three=three"""

MULT_BAD_CACHED = {'one': 'one', 'three': '3'}

EXPECTED_MULT_BAD_CACHED = """#
# 2 ERRORS found when upgrading cache file!
#

# ERROR: Failed validation! type: int, regex: \d+
# Help statement 1
one=one

# NOTE: No default/cached value specified!
# Help statement 2
two=

# ERROR: Failed validation! type: str, regex: [a-zA-Z]+
# Help statement 3
three=3"""


class TestIpconfGen(unittest.TestCase):

    def create_test_file(self, content):
        """Used to create a test configvars file."""
        fh, filename = tempfile.mkstemp()
        self.addCleanup(os.remove, filename)
        os.close(fh)
        with open(filename, 'w') as fh:
            fh.write(content)
        return filename

    def test_get_file_id_with_macro(self):
        """Test that get_file_id can pull the macro from a file."""
        filename = self.create_test_file(FILE_WITH_MACRO)
        macro_output = ipconf_gen.get_file_id(filename)
        self.assertEqual(macro_output, 'macro_text')

    def test_get_file_id_without_macro(self):
        """Test get_file_id's response when the macro isn't in the file."""
        filename = self.create_test_file(FILE_WITHOUT_MACRO)
        macro_output = ipconf_gen.get_file_id(filename)
        expected = '%s:%s' % (socket.gethostname(), filename)
        self.assertEqual(macro_output, expected)

    def test_gen_header(self):
        """Test that gen_header produces a properly formatted header."""
        file_id = 'file_id'
        md5 = '1234567890abcdef'
        user = pwd.getpwuid(os.getuid())[0]
        pattern = '^# User %s created this file from %s on [A-Za-z]+ ' \
                  '[A-Za-z]+\s+\d+ \d+\:\d+\:\d+ \d+\n%s=%s\n\n$' % (user,
                  file_id, ipconf.CONF_VERSION_CHECKSUM_VAR, md5)
        output = ipconf_gen.gen_header(file_id, md5)
        self.assertTrue(re.match(pattern, output))

    def gen_file_content_tester(self, configvars_data, cached_data, expected):
        """Tester function for ipconf_gen.gen_file_content function."""
        configvars = []
        for var in configvars_data:
            configvars.append(configbuilder.ConfigVariable(**var))
        file_content = ipconf_gen.gen_file_content(configvars, cached_data)
        self.assertEqual(file_content, expected)

    def test_gen_file_content(self):
        """Test gen_file_content generates proper content from configvars."""
        self.gen_file_content_tester(CONFIGVARS, None, EXPECTED)

    def test_gen_file_content_upgrade_correct(self):
        """Test gen_file_content with a correct set of upgrade cached vals."""
        self.gen_file_content_tester(CONFIGVARS, CORRECT_CACHED, EXPECTED_CACHED)

    def test_gen_file_content_upgrade_bad(self):
        """Test gen_file_content with a bad set of upgrade cached vals."""
        self.gen_file_content_tester(CONFIGVARS, BAD_CACHED, EXPECTED_BAD_CACHED)

    def test_gen_file_content_upgrade_bad_mult(self):
        """Test gen_file_content with a bad set of upgrade cached vals."""
        self.gen_file_content_tester(CONFIGVARS, MULT_BAD_CACHED,
                                     EXPECTED_MULT_BAD_CACHED)


if __name__ == '__main__':
    unittest.main()
