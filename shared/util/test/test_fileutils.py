import errno
import os
import tempfile
import unittest2 as unittest

from shared.util import fileutils


KV_FILE_DICT = {'1' : '111',
                '2' : '222',
                '3' : '333'}

KV_FILE_TEXT = """1=111
3=333
2=222"""

KV_FILE_TEXT_INCORRECT = """1=111
3:333
2 222"""

FILE_TEXT = """111
222
333"""

ENCRYPTION_MOCKUP = {
    'key' : '748314ebcb81e8aca691c0cfed003de1',
    'iv' : '2f611b524c8caef3',
    'text' : FILE_TEXT,
    'enc_text' : '\xa4\xa8\x14\x08y\x16\xc6\xae>\x8e\n\xa3\x89e\x106',
}


class OSMockup:

    def __init__(self):
        self.F_OK = 0
        # List for saving calls order
        self.call_log = list()

    def access(self, fname, *args):
        self.call_log.append('access')
        if fname in ('true', 'raise'):
            # Deep into 'nfs_rename' function logic.
            # 'true' - normal function flow
            # 'raise' - exception handling flow
            return True
        else:
            return False

    def remove(self, fname, *args):
        self.call_log.append('remove')
        if fname == 'true.old':
            # Normal flow
            return
        else:
            # Exception handling flow
            err = OSError()
            err.errno = errno.ENOENT
            raise err

    def link(self, *args):
        self.call_log.append('link')

    def rename(self, *args):
        self.call_log.append('rename')


class TestFileUtils(unittest.TestCase):

    def setUp(self):
        fhandle, self.fname = tempfile.mkstemp()
        os.close(fhandle)

    def tearDown(self):
        if os.path.exists(self.fname):
            os.remove(self.fname)

    def test_nfs_close(self):
        #Just a code runner
        fhandle = open(self.fname, 'w')
        self.assertEquals(fileutils.nfs_close(fhandle), None)

    def test_nfs_rename_logic(self):
        # Try to test existing logic of nfs_rename
        os_mock = OSMockup()
        orig_os = fileutils.os
        fileutils.os = os_mock
        try:
            # Normal function flow
            fileutils.nfs_rename('', 'true')
            self.assertEquals(os_mock.call_log, ['access', 'remove',
                                                 'link', 'rename'])

            # Exception handling flow
            os_mock.call_log = list()
            fileutils.nfs_rename('', 'raise')
            self.assertEquals(os_mock.call_log, ['access', 'remove',
                                                 'link', 'rename'])

            # Alternative function flow
            os_mock.call_log = list()
            fileutils.nfs_rename('', 'ababa')
            self.assertEquals(os_mock.call_log, ['access', 'rename'])
        finally:
            fileutils.os = orig_os

    def test_nfs_rename_result(self):
        # Try to test nfs_rename on local file system
        tmp, dest_fname = tempfile.mkstemp()
        os.close(tmp)

        fhandle = open(dest_fname, 'w')
        try:
            fhandle.write(KV_FILE_TEXT_INCORRECT)
        finally:
            fhandle.close()

        fileutils.nfs_rename(dest_fname, self.fname, oldext='.old')

        fhandle = open(self.fname)
        try:
            self.assertEquals(open(self.fname).read(), KV_FILE_TEXT_INCORRECT)
        finally:
            fhandle.close()
        os.remove(self.fname + '.old')

    def test_write_kv_file(self):
        fileutils.write_kv_file(self.fname, KV_FILE_DICT, 'Test')
        fhandle = open(self.fname, 'r')
        kv_file = ''.join(fhandle.readlines())
        fhandle.close()
        self.assertEquals(kv_file, '\n'.join(('# Test', KV_FILE_TEXT)))

    def test_read_kv_file(self):
        fhandle = open(self.fname, 'w')
        fhandle.write('%s\n%s' % (KV_FILE_TEXT, '# Just a comment.'))
        fhandle.close()

        result = fileutils.read_kv_file(self.fname)
        self.assertEquals(len(result), len(KV_FILE_DICT))
        for r in result:
            self.assertEquals(result[r], KV_FILE_DICT[r])

    def test_read_kv_file_incorrect(self):
        fhandle = open(self.fname, 'w')
        fhandle.write(KV_FILE_TEXT_INCORRECT)
        fhandle.close()

        self.assertRaises(fileutils.EncodeError,
                          fileutils.read_kv_file,
                          self.fname)

        os.remove(self.fname)
        self.assertRaises(IOError,
                          fileutils.read_kv_file,
                          self.fname)

        self.assertEquals(fileutils.read_kv_file(self.fname,
                                                 silent=True),
                          dict())

    def test_gzip(self):
        fhandle = open(self.fname, 'w')
        fhandle.write(FILE_TEXT)
        fhandle.close()
        fileutils.compress_gzip(self.fname)
        fileutils.decompress_gzip(self.fname)
        fhandle = open(self.fname)
        self.assertEquals(''.join(fhandle.readlines()), FILE_TEXT)
        fhandle.close()

    def test_encryption(self):
        fhandle = open(self.fname, 'w')
        fhandle.write(ENCRYPTION_MOCKUP['text'])
        fhandle.close()

        tmp, dest_fname = tempfile.mkstemp()
        os.close(tmp)

        fileutils.encrypt_file(self.fname, dest_fname,
                               ENCRYPTION_MOCKUP['key'],
                               iv=ENCRYPTION_MOCKUP['iv'])
        encrypted = ''.join(open(dest_fname).readlines())
        self.assertEquals(encrypted, ENCRYPTION_MOCKUP['enc_text'])

        fileutils.decrypt_file(dest_fname, self.fname,
                               ENCRYPTION_MOCKUP['key'],
                               iv=ENCRYPTION_MOCKUP['iv'])
        os.remove(dest_fname)
        decrypted = ''.join(open(self.fname).readlines())
        self.assertEquals(decrypted, ENCRYPTION_MOCKUP['text'])


if __name__ == '__main__':
    unittest.main()

