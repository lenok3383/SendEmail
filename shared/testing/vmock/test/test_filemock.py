"""FileMock library test.

:Status: $Id: //prod/main/_is/shared/python/testing/vmock/test/test_filemock.py#1 $
:Authors: gmazzola
"""

import unittest2

from shared.testing.vmock import mockcontrol, matchers
from shared.testing.vmock.helpers.filemock import FileMock

CONTENT = """The quick
brown fox

jumps
over the
lazy dog."""

class TestFileMock(unittest2.TestCase):

    def setUp(self):
        self.mc = mockcontrol.MockControl()

    def test_with_read(self):
        """Test file reads inside a `with' statement."""
        with FileMock(CONTENT) as f:
            content = f.read()

        self.assertEquals(CONTENT, content)

    def test_with_iterate(self):
        """Test iterating over a file inside a `with' statement."""
        content = ''
        with FileMock(CONTENT) as f:
            for line in f:
                content += line

        self.assertEquals(CONTENT, content)

    def test_partial_read(self):
        """Test partial f.read() calls."""
        f = FileMock(CONTENT)

        part1 = f.read(8)
        self.assertEquals('The quic', part1)

        part2 = f.read(7)
        self.assertEquals('k\nbrown', part2)

        part3 = f.read()
        content = part1 + part2 + part3

        self.assertEquals(CONTENT, content)

    def test_readline(self):
        """Test the readline() method."""
        f = FileMock(CONTENT)
        CORRECT_LINES = ['The quick\n', 'brown fox\n', '\n', 'jumps\n',
                         'over the\n', 'lazy dog.', '', '', '']

        for correct_line in CORRECT_LINES:
            line = f.readline()
            self.assertEquals(correct_line, line)

    def test_seek_read(self):
        """Test file seeking."""
        f = FileMock(CONTENT)
        f.seek(6)
        line1 = f.readline()
        self.assertEquals('ick\n', line1)

        f.seek(0)
        line2 = f.readline()
        self.assertEquals('The quick\n', line2)

    def test_tell(self):
        """Test file position telling."""
        f = FileMock(CONTENT)
        f.seek(6)
        self.assertEquals(6, f.tell())

    def test_truncate(self):
        """Test file truncation."""
        f = FileMock(CONTENT, writeable=True)
        line = f.readline()
        f.truncate()
        f.seek(0)

        self.assertEquals(line, f.read())

    def test_write(self):
        """Test file writing."""
        f = FileMock('', writeable=True)
        f.write('Hello.')
        f.seek(0)

        line = f.read()
        self.assertEquals('Hello.', line)

    def test_writelines(self):
        """Test the writelines method."""
        f = FileMock('', writeable=True)
        f.writelines(['hello', ' ', 'world'])
        f.seek(0)

        self.assertEquals('hello world', f.read())

    def test_not_writeable(self):
        """Test that writes fail when the file was not opened for writing."""
        f = FileMock('', writeable=False)
        self.assertRaises(IOError, f.write, 'hello')


if __name__ == '__main__':
    unittest2.main()
