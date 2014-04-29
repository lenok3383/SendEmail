"""FileMock class that helps mock open() calls.

:Author: gmazzola
:Status: $Id: //prod/main/_is/shared/python/testing/vmock/helpers/filemock.py#1 $

Using FileMock
--------------

Unfortunately you cannot use VMock to mock internal functions, such as 'open'.
However, you can create a wrapper for open:

def wrap_open(fname, mode='r'):
    return open(fname, mode)

In the file you're testing, you will need to change all calls from open() to
wrap_open(). You can then use VMock to mock the calls to wrap_open as follows:

content = 'hello world\n'
file = FileMock(content)

open_mock = self.mc.mock_method(obj, 'wrap_open')
open_mock('/path/to/file').returns(file)

FileMock vs StringIO
--------------------

StringIO seems like a perfect replacement for FileMock -- it allows you to treat
a string as if it were a file object. However, it doesn't support the important
"with ... as f" syntax. See http://bugs.python.org/issue1286 for details.
"""

class FileMock(object):
    def __init__(self, content, writeable=False, fd=0, fname=None):
        """FileMock constructor.

        :param content: The contents of the file.
        :param writeable: Can we write to this file?
        :param fd: The file descriptor, in case your program calls f.fileno().
        :param fname: The file name, in case your program calls f.name.
        """
        self.content = content
        self.writeable = writeable
        self.bytes_read = 0

        self.closed = False
        self.fd = fd
        self.encoding = None
        self.errors = None
        self.mode = 'w' if writeable else 'r'
        self.name = fname
        self.newlines = None
        self.softspace = 0

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        return False

    def __iter__(self):
        return self

    def close(self):
        self.closed = True

    def fileno(self):
        return self.fd

    def flush(self):
        return

    def isatty(self):
        return False

    def next(self):
        """Iterate over the lines in the file"""
        if self.closed:
            raise ValueError('I/O operation on closed file')

        if self.bytes_read == len(self.content):
            raise StopIteration
        else:
            return self.readline()

    def read(self, count=None):
        """Read `count' bytes from the file."""
        start = self.bytes_read

        if count is None or start + count >= len(self.content):
            count = len(self.content) - start

        self.bytes_read += count
        return self.content[start : start + count]

    def readline(self):
        """Read a line from the file."""
        start = self.bytes_read
        end = self.content[self.bytes_read:].find('\n')
        if end == -1:
            self.bytes_read = len(self.content)
            return self.content[start:]
        else:
            # Include the \n in the returned line.
            self.bytes_read += end + 1
            return self.content[start : start + end + 1]

    def readlines(self):
        """Read every line from the file."""
        lines = self.content[self.bytes_read:].split('\n')

        # Include the \n in the returned lines.
        for i in xrange(len(lines) - 1):
            lines[i] += '\n'

        self.bytes_read = len(self.content)
        return lines

    def seek(self, pos):
        """Seek to a position in the file."""
        self.bytes_read = pos

    def tell(self):
        """Return the current file position."""
        return self.bytes_read

    def truncate(self, pos=None):
        """Truncate the file."""
        if not self.writeable:
            raise IOError('File not open for writing')

        if pos is None:
            pos = self.bytes_read

        self.content = self.content[:pos]

    def write(self, line):
        """Write to the file."""
        if not self.writeable:
            raise IOError('File not open for writing')

        self.content += line

    def writelines(self, lines):
        """Write a list of lines to the file."""
        for line in lines:
            self.write(line)
