"""Rotating file utilities.

:Status: $Id: //prod/main/_is/shared/python/file/rotating_file.py#2 $
:Author: ivlesnik
:Last Updated By: $Author: duncan $
"""

import logging
import os
import time

from shared.util.fileutils import nfs_close

# Day constant.
ONE_DAY = 86400


class DatedFileNameRotator(object):

    """File name rotating class.

    Create a new file name periodically. The default interval is 1-day.
    You are supposed to provide the prefix of the file name, which usually
    should include the full path. Something like /root/foo/log/mylog-.
    The file name returned will be /root/foo/log/mylog-2004-08-21
    """

    def __init__(self, prefix, start_ts, end_ts=None, interval=ONE_DAY):
        """Constructor.

        :Parameters:
          `prefix`: the prefix used to genereate the file name.
          `start_ts`: the initial timestamp to start the rotation from.
          `end_ts`: the final timestamp to end the rotation or None for
              never-ending rotation. Default is None.
          `interval`: rotation interval. Default is 86400 seconds (one day).
        """

        self._prefix = prefix
        self._start_ts = start_ts
        self._end_ts = end_ts
        self._interval = interval
        self._file_key = None

    def __iter__(self):
        return self

    @property
    def key(self):
        """Get file key."""

        return self._file_key

    @key.setter
    def key(self, file_key):
        """Set file key."""

        self._file_key = file_key

    @property
    def interval(self):
        """Get interval."""

        return self._interval

    @property
    def start_ts(self):
        """Get start timestamp."""

        return self._start_ts

    @property
    def end_ts(self):
        """Get end timestamp."""

        return self._end_ts

    def next(self):
        """The next available file.

        :Return: next file name if available or None.
        """

        if self._file_key is None:
            self._file_key = self._start_ts
        else:
            self._file_key += self._interval

        if self._end_ts is not None and self._file_key > self._end_ts:
            self._file_key = None
            raise StopIteration

        return self._generate_file_name(self._file_key)

    def curr(self):
        """Return current file name if it exists.

        Returns None if it has not been initialized. The current file key
        will be initialized by last successful next() call.
        """

        if not self._file_key:
            return None
        file_name = self._generate_file_name(self._file_key)
        return file_name

    def is_current(self, file_name):
        """Test whether the given file name is the current rotator's file name.

        :Parameters:
          `file_name`: file name to test.
        """

        return file_name == self.curr()

    def _generate_file_name(self, file_key):
        """Build file name by the key value and self._prefix.

        :Parameters:
          `file_key`: the key identifier of the file (unix timestamp).
        """

        time_struct = time.gmtime(file_key)

        if self._interval >= ONE_DAY:
            file_name = '%s%04d-%02d-%02d' % (
                self._prefix, time_struct.tm_year, time_struct.tm_mon,
                time_struct.tm_mday)
        else:
            file_name = '%s%04d-%02d-%02d-%02d%02d' % (
                self._prefix, time_struct.tm_year, time_struct.tm_mon,
                time_struct.tm_mday, time_struct.tm_hour, time_struct.tm_min)

        return file_name


class RotatingFileReaderError(Exception):

    """Exception raised in RotatingFileReader class."""

    pass


class RotatingFileReader(object):

    """RotatingFileReader class."""

    def __init__(self, file_name_rotator):
        """Constructor.

        :Parameters:
            `file_name_rotator`: an instance of DatedFileNameRotator object.
        """

        self.rotator = file_name_rotator
        self._file = None
        self._log = logging.getLogger()

    def __del__(self):
        self.close()

    def open_next_file(self):
        """Attempt to open file to read."""

        if self._file:
            self.close()
        try:
            file_name = self.rotator.next()
            # Skip non-existent files.
            while not os.path.exists(file_name):
                self._log.warn('The next expected file does not exist: %s',
                    file_name)
                file_name = self.rotator.next()
            self._file = open(file_name, 'r')
            return file_name
        except StopIteration:
            self.close()
            return None

    def read(self, size):
        """Reads from the current file.

        Returns upto "size" bytes of data from current file. It does not read
        accross the files.  However, it will attempt to read next file if the
        current file has already reached the end.  The returned string could be
        less than the desired size because the end of current file has been
        reached. Returns None of no more data available and the file pointer
        will rest at the end of the current file.

        :Parameters:
            `size`: number of bytes of data to read from the current file.

        :Return: data chunk read from the current file.
        """

        if not self._file:
            self.open_next_file()

        data = self._file.read(size) or None
        if data is None:
            # Reached the end of current file.
            self.close()
            _file = self.open_next_file()
            if _file:
                data = self._file.read(size) or None

        return data

    def read_line(self):
        """Read next line.

        :Return: line read from the current file or None if EOF.
        """

        if not self._file:
            self.open_next_file()

        line = self._file.readline() or None
        if not line:
            # Reached the end of current file.
            self.close()
            _file = self.open_next_file()
            if _file:
                line = self._file.readline() or None

        return line

    def seek(self, pos):
        """Seek to an absolute position in the current file.

        :Parameters:
            `pos`: byte position for seek.
        """

        if not self._file:
            self.open_next_file()

        if pos is None:
            pos = 0

        self._file.seek(pos)

    def size(self):
        """Return size in bytes of the current file.
        If file does not exist raises an exception.

        :Return: the size in bytes of the file.

        :Raises RotatingFileReaderError: for unexistent file.
        """

        if not self._file:
            self.open_next_file()

        file_name = self.curr()
        if not os.path.exists(file_name):
            raise RotatingFileReaderError('File %s does not exist', file_name)

        return os.stat(file_name).st_size

    def tell(self):
        """Returns a tuple of the current file name and the file pointer
        position or None if not valid file has been opened yet.
        """

        if self._file:
            return (self.curr(), self._file.tell())
        else:
            return None

    def curr(self):
        """Return the current file name from the underlying rotating reader."""

        return self.rotator.curr()

    def close(self):
        """Close the file that currently opened."""

        if self._file:
            self._file.close()
            self._file = None


class RotatingFileWriterError(Exception):

    """Exception raised in RotatingFileWriter class."""

    pass


class RotatingFileWriter(object):

    """RotatingFileWriter class."""

    def __init__(self, file_name_rotator):
        """Class constructor.

        :Parameters:
            `file_name_rotator`: DatedFileNameRotator instance.
        """

        self.rotator = file_name_rotator
        self._file = None
        self._filename = None

    def is_closed(self):
        """Returns True if the writer is closed, or False otherwise."""

        return self._file is None

    def open_next_file(self):
        """Attempt to open the next available file to write.

        :Returns: the current file name or None if no more files available.
        """

        if not self.is_closed():
            self.close()
        try:
            file_name = self.rotator.next()
            self._file = open(file_name, 'a')
            self._filename = file_name
            return file_name
        except StopIteration:
            return None

    def _write(self, string):
        """Write string to the open file.

        :Returns: (filename, start_pos, end_pos) of written string.
        """
        start_pos = self._file.tell()
        self._file.write(string)
        end_pos = self._file.tell()
        return (self._filename, start_pos, end_pos)

    def write(self, string):
        """Write a string to the current file.

        It doesn't allow writing to the file tagged by the past timestamp.
        To write to such 'past' file use write_by_timestamp method instead.

        :Parameters:
            `string`: text string to be written to the file.

        :Returns: (filename, start_pos, end_pos) of written string.
        """

        # Open the next file if the method called for the first time.
        if self.is_closed():
            if not self.open_next_file():
                return

        current_time = time.time()
        if current_time < self.rotator.key + self.rotator.interval:
            # Write the string to the current file.
            return self._write(string)
        else:
            # If we passed the rotator's end timestamp, then close the file,
            # which will signalize that the writer finished with all files.
            if self.rotator.end_ts is not None and \
                    current_time > self.rotator.end_ts:
                self.close()
            else:
                # Write to a file identified by the current timestamp.
                return self.write_by_timestamp(current_time, string)

    def write_by_timestamp(self, timestamp, string):
        """Write a string to the file specified by timestamp.

        The timestamp may be any time value between the rotator's start_ts and
        end_ts properties. The appropriate file_key is detected here.

        :Parameters:
            `string`: text string to be written to the file.
            `timestamp`: the timestamp to identify the required output file.

        :Raises: RotatingFileWriterError when the given timestamp is
            outside the rotator time boundaries.

        :Returns: (filename, start_pos, end_pos) of written data.
        """

        if timestamp < self.rotator.start_ts or \
                timestamp > self.rotator.end_ts and \
                self.rotator.end_ts is not None:
            raise RotatingFileWriterError('The file timestamp is outside'\
                ' the rotator time boundaries')

        # Detect the desired file key.
        step = (timestamp - self.rotator.start_ts) / self.rotator.interval
        file_key = self.rotator.start_ts + self.rotator.interval * int(step)

        # Open required file if it is not the current one.
        if file_key != self.rotator.key:
            self.close()
            self.rotator.key = file_key
            self._file = open(self.rotator.curr(), 'a')

        # Write string to file.
        return self._write(string)

    def close(self):
        """Close the file that currently opened."""

        if self._file:
            nfs_close(self._file)
            self._file = None

    def flush(self):
        """Flush the internal I/O buffer to the current file."""

        if self._file:
            self._file.flush()
