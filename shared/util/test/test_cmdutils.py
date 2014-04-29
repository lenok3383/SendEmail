
import os
import shutil
import sys
import tempfile
import unittest2

from shared.util import cmdutils


STD = []

class Std:
    """Mockup class for stdout."""

    def write(self, smth):
        """Function to write into a list instead of std output."""
        smth = str(smth).strip()
        if smth:
            STD.append(smth)


test_file = """
class test_command:

    help = 'Test help'
    usage = 'Test usage'

    def __call__(self, args):
        print('Test function is called.')
        if args[0] == 'raise':
            raise Exception
        return args
"""


commands = ['test_command']

class Test(unittest2.TestCase):
    """Test case class to test cmdutils module."""

    def setUp(self):
        self.path = tempfile.mkdtemp(prefix='test_')
        sys.path.insert(0, os.path.dirname(self.path))
        self.package = os.path.basename(self.path)
        self.__create_file(os.path.join(self.path, '__init__.py'), '')
        self.__create_file(os.path.join(self.path, 'test_command.py'),
                                        test_file)
        self.cmd = cmdutils.ClassBasedCmd(self.package, commands)
        self.std_orig = cmdutils.sys.stdout
        self.std_orig_cmd = self.cmd.stdout
        cmdutils.sys.stdout = self.cmd.stdout = Std()

    def tearDown(self):
        shutil.rmtree(self.path, ignore_errors=True)
        sys.path.remove(os.path.dirname(self.path))
        cmdutils.sys.stdout = self.std_orig
        self.cmd.stdout = self.std_orig_cmd
        global STD
        STD = []

    def test_onecmd_help1(self):
        self.cmd.onecmd('help test_command')
        self.assertTrue('Test help' in STD)
        self.assertTrue('Test usage' in STD)
        self.assertTrue('Test function is called.' not in STD)
        self.assertEqual(self.cmd.rc, 0)

    def test_onecmd_help2(self):
        self.cmd.onecmd('help')
        self.assertTrue('Usage:\n  help\n  help <command>\n\nCommands:' in STD)
        self.assertTrue('test_command -- Test help' in STD)
        self.assertEqual(self.cmd.rc, 0)

    def test_onecmd_error(self):
        self.cmd.onecmd('wrong_command')
        self.assertTrue('*** Unknown command: wrong_command' in STD)
        self.assertEqual(self.cmd.rc, 1)

    def test_onecmd(self):
        print(self.cmd.onecmd('test_command args'))
        self.assertTrue('Test function is called.' in STD)
        self.assertTrue("['args']" in STD)
        self.assertEqual(self.cmd.rc, 0)

    def test_onecmd_onecmd_raise(self):
        self.assertRaises(Exception, self.cmd.onecmd, 'test_command raise')
        self.assertEqual(self.cmd.rc, 1)

    def __create_file(self, path, content):
        f = open(path, 'w')
        try:
            f.write(content)
        finally:
            f.close()

if __name__ == "__main__":
    unittest2.main()
