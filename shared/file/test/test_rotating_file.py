"""Unit tests for shared.file.rotating_file module.

:Status: $Id: //prod/main/_is/shared/python/file/test/test_rotating_file.py#2 $
:Author: ivlesnik
:Last Modified By: $Author: duncan $
"""

import logging
import os
import shutil
import sys
import tempfile
import time
import unittest2

from shared.file.rotating_file import (DatedFileNameRotator,
        RotatingFileReader, RotatingFileWriter)


class TestRotatingFile(unittest2.TestCase):

    """rotating_file Module Test Suite."""

    def setUp(self):
        """Make setup for each unit test."""

        self.test_dir = tempfile.mkdtemp(prefix='shared_ut_')
        self.rotator_prefix = os.path.join(self.test_dir, 'log-')
        self._log = logging.getLogger()
        self._log.setLevel(logging.DEBUG)
        if not self._log.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter(
                '%(asctime)s %(levelname)s: %(message)s'))
            self._log.addHandler(handler)

    def tearDown(self):
        """Cleanup required after each test."""

        if os.path.isdir(self.test_dir):
            shutil.rmtree(self.test_dir)
            self.test_dir = None

    def test_01_dated_rotator(self):
        """Test DatedFileNameRotator methods."""

        start_ts = time.time() - 3600
        end_ts = time.time() + 3600
        interval = 600

        rotator = DatedFileNameRotator(
            prefix=self.rotator_prefix,
            start_ts=start_ts,
            end_ts=end_ts,
            interval=interval)

        # Test curr() and next() methods.
        time_format = '%s%04d-%02d-%02d-%02d%02d'
        _time = start_ts
        while _time <= end_ts:
            time_struct = time.gmtime(_time)
            expected_file_name = time_format % (
                self.rotator_prefix, time_struct.tm_year,
                time_struct.tm_mon, time_struct.tm_mday,
                time_struct.tm_hour, time_struct.tm_min)
            try:
                actual_file_name = rotator.next()
                self.assertEqual(actual_file_name, expected_file_name)
                self.assertEqual(rotator.curr(), expected_file_name)
                _time += interval
            except StopIteration:
                break

        # Test is_current() method.
        for file_name in rotator:
            self.assertTrue(rotator.is_current(file_name))

    def test_02_rotating_file_reader_writer(self):

        interval = 60
        current_time = time.time()
        start_ts = current_time - interval * 2
        end_ts = current_time + interval
        expected_files_number = 3

        ##########################
        # Test RotatingFileWriter.
        ##########################
        write_rotator = DatedFileNameRotator(
            prefix=self.rotator_prefix,
            start_ts=start_ts,
            end_ts=end_ts,
            interval=interval)

        saved_time_func = time.time
        time_list = [current_time +  index for index in xrange(180)]
        time_list.reverse()
        # Assign the mock list to time.time() function.
        time.time = time_list.pop

        try:
            # Allow 15 minutes ahead of now.
            writer = RotatingFileWriter(write_rotator)

            i = 0
            while True:
                i = i + 1
                if i == 10:
                    text = 'MARK'
                else:
                    text = time.asctime(time.gmtime())
                writer.write("%d\t%s\n" % (i, text))
                if writer.is_closed():
                    break
        finally:
            writer.close()
            time.time = saved_time_func

        # Test write_by_timestamp.
        test_line = "The final line of the first file\n"
        writer.write_by_timestamp(start_ts + 30, test_line)
        writer.flush()

        # Check if expected number of files were created.
        self.assertEqual(len(os.listdir(self.test_dir)), expected_files_number)

        ##########################
        # Test RotatingFileReader.
        ##########################
        read_rotator = DatedFileNameRotator(
            prefix=self.rotator_prefix,
            start_ts=start_ts,
            end_ts=end_ts,
            interval=interval)
        reader = RotatingFileReader(read_rotator)

        # Test size of the first file.
        self.assertGreater(reader.size(), 0)

        mark = '10\tMARK\n'
        file_path = None
        pos = None
        test_line_found = False
        try:
            i = 0
            while True:
                line = reader.read_line()
                if line is None:
                    break
                i = i + 1
                if i == 10:
                    # Test tell() method.
                    file_path, pos = reader.tell()
                    self.assertEqual((file_path, pos), (reader.curr(), 243))
                elif i == 11:
                    # Here we should find the mark saved by RotatingFileWriter.
                    self.assertEqual(line, mark)
                    break
                elif line == test_line:
                    test_line_found = True

            # Check if the line written by write_by_timestamp method was found.
            self.assertTrue(test_line_found)

            # Test seek().
            self.assertRaises(IOError, reader.seek, -1)
            self.assertIsNone(reader.seek(pos))

            # Test read.
            data = reader.read(len(mark))
            self.assertEqual(data, mark)
            data_found = False
            while True:
                data = reader.read(1000)
                if not data:
                    break
                self.assertLessEqual(len(data), 1000)
                data_found = True

            # Test that at least something was read from the file.
            self.assertTrue(data_found)

        finally:
            reader.close()

    def test_write_returns_pos(self):
        start_ts = time.time()
        write_rotator = DatedFileNameRotator(
            prefix=self.rotator_prefix,
            start_ts=start_ts)
        writer = RotatingFileWriter(write_rotator)

        expected_filename = self.rotator_prefix + \
          time.strftime('%Y-%m-%d', time.gmtime())
        self.assertEqual(writer.write('hello world'),
                         (expected_filename, 0, 11))


if __name__ == "__main__":
    unittest2.main()
