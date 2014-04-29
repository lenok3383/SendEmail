"""File and filename utilities

During the porting to Python 2.6 we've made the following changes
which break compatibility with existing IronPort products:

    - compress renamed to compress_gzip,
    - decompress renamed to decompress_gzip.

:Status: $Id: //prod/main/_is/shared/python/util/fileutils.py#9 $
:Authors: wesc, kylev
"""

import binascii
import errno
import gzip
import os
import sys

CORO = os.environ.get('CORO_ENABLE') in ('1', 'True')

if CORO:
    import coro
    from ssh.cipher import des3_cbc
else:
    try:
        from Crypto.Cipher import DES3
    except ImportError:
        pass


CHUNK_SIZE = 65536
COMPRESS_CHUNK = 10 * 1024 * 1024


class FileError(OSError):
    """Base exception class for fileutils errors."""


class EncodeError(FileError):
    """Raised if file isn't in the right format."""


def nfs_rename(src, dest, oldext=".old"):
    """An "NFS safe-er" rename.

    On local filesystems, you can do an os.rename() to replace a file with
    a newer one and active filehandles will stay valid.  On NFS, we can use
    a hard link to help keep existing NFS filehandles open.  Open filehandles
    will still fail if you call nfs_rename() more than once while the
    destination file is being read.  Hopefully you can read faster than that.

    :param src: Path to source file.
    :param dest: Destination path.
    :param oldext: Extension for a temporary hard link.
    """
    # Only be 'safe' if dest exists
    if os.access(dest, os.F_OK):
        oldfile = '%s%s' % (dest, oldext)

        try:
            os.remove(oldfile)
        except OSError as err:
            # Ignore a non-existing 'old file'
            if err.errno != errno.ENOENT:
                raise

        # Create a hard link go avoid disrupting active NFS filehandles
        os.link(dest, oldfile)

    # Move new file into place
    os.rename(src, dest)


def nfs_close(fhandle):
    """An "NFS safe-er" close.

    It flushes and syncs the file object before attempting to do fh.close().
    This seems to solve the problem with EBADF on close().

    :param fhandle: File object to be closed.
    """
    fhandle.flush()
    os.fsync(fhandle.fileno())
    fhandle.close()


def encrypt_file(src, dest, key, iv=None, block_size=1, **kwargs):
    """Encrypts a file, using des3_cbc cipher.

    Note: Requires coro/godspeed to import ssh.ciphers.

    :param src: Unencrypted source file.
    :param dest: Encrypted destination file.  Will be truncated if it exists.
    :param key: Encryption key suitable for des3_cbc method.
    :param iv: Initialization vector.
    :param block_size: Block size for cipher.
    :param **kwargs: Used to support single **kwargs dictionary
                     for different file transformations.
    """

    src_fp = open(src, 'r')
    dest_fp = open(dest, 'w')

    try:
        # Set up the des3_cbc cipher.
        if CORO:
            cipher = des3_cbc.Triple_DES_CBC()
            cipher.block_size = block_size
            cipher.set_encryption_key_and_iv(binascii.unhexlify(key),
                                                binascii.unhexlify(iv))
        else:
            DES3.block_size = block_size
            cipher = DES3.new(binascii.unhexlify(key),
                              DES3.MODE_CBC,
                              binascii.unhexlify(iv))

        # Read through the unencrypted source file.
        done = False
        while not done:
            data = src_fp.read(CHUNK_SIZE)
            if len(data) < CHUNK_SIZE or len(data) == 0:
                # Finished reading data.  Do padding magic to make sure
                # the file size is a multiple of 8.
                padding_len = 8 - len(data) % 8
                data += binascii.unhexlify(('0' + str(padding_len)) *
                        padding_len)
                done = True
            dest_fp.write(cipher.encrypt(data))
    finally:
        src_fp.close()
        dest_fp.close()


def decrypt_file(src, dest, key, iv=None, block_size=1, **kwargs):
    """Decrypts a file encrypted using encrypt_file() and des3_cbc cipher.

    Note: Requires coro/godspeed to import ssh.ciphers.

    :param src: Encrypted source file.
    :param dst: Decrypted destination file.  Will be truncated if it exists.
    :param key: Encryption key suitable for des3_cbc method.
    :param iv: Initialization vector.
    :param block_size: Block size for cipher.
    :param **kwargs: Used to support single **kwargs dictionary
                     for different file transformations.
    """

    src_fp = open(src, 'r')
    dest_fp = open(dest, 'w')

    try:
        # Open the file and decrypt it.
        if CORO:
            cipher = des3_cbc.Triple_DES_CBC()
            cipher.block_size = block_size
            cipher.set_encryption_key_and_iv(binascii.unhexlify(key),
                                             binascii.unhexlify(iv))
        else:
            DES3.block_size = block_size
            cipher = DES3.new(binascii.unhexlify(key),
                              DES3.MODE_CBC,
                              binascii.unhexlify(iv))

        file_size = os.path.getsize(src)
        bytes_read = 0
        done = False
        while not done:
            data = src_fp.read(CHUNK_SIZE)
            bytes_read += len(data)
            decrypt_string = cipher.decrypt(data)
            if bytes_read >= file_size:
                # Remove padding bytes.  The file is padded with bytes all
                # of the same value as the number of padding bytes (as per
                # PKCS5, RFC2630, and NIST 800-38a).
                padding_bytes = int(binascii.hexlify(decrypt_string[-1]))
                decrypt_string = decrypt_string[:-(padding_bytes)]
                done = True
            dest_fp.write(decrypt_string)
    finally:
        src_fp.close()
        dest_fp.close()


def read_kv_file(filename, silent=False):
    """Read a "key=value" file.

    A file must have the next format:
    # comment
    <key1>=<value1>
    <key2>=<value2>
    ...
    Values will be read as strings.

    :param filename: Path to the file to read.
    :param silent: Check whether file exists before trying anything.
                   If it's not, fail silently and return an empty dict.

    :return: Dictionary like {key1: value1, key2: value2, ...}
    :raise: EncodeError if file is not in the right format.
    """

    try:
        val_by_key = dict()
        for line in file(filename):
            line = line.strip()
            if not line.startswith('#'):
                if line.find('=') != -1:
                    key, value = line.split('=')
                    val_by_key[key.strip()] = value.strip()
                else:
                    raise EncodeError("Can't read '%s' as kv_file." % \
                                      (filename,))
        return val_by_key
    except (IOError, OSError):
        if silent:
            return dict()
        else:
            raise


def write_kv_file(filename, kv_dict, header_comment=None):
    """Write a "key=value" file that is readable by read_kv_file function.

    key=value pair output is not order-stable.  Do not rely on the order of
    records in the output file.

    :param filename: Destination filename.
    :param kv_dict: Dictionary to write.  Values must be str() converted.
    :param header_comment: Optional string to write as a leading #-denoted
                           comment.
    """
    fhandle = open(filename, 'w')

    try:
        lines = list()
        if header_comment:
            lines.append('# %s' % (header_comment,))
        lines.extend(['%s=%s' % (key, val) for key, val
                                           in kv_dict.iteritems()])
        fhandle.write('\n'.join(lines))
    finally:
        fhandle.close()


def compress_gzip(filename):
    """Compress one file "in place" using gzip.

    :param filename: Path to file to compress.
    :raise: NotFoundError if file is not found.
    """

    fhandle = open(filename)
    gzip_file = gzip.GzipFile(filename + '.gz', 'w')
    try:
        while True:
            data = fhandle.read(COMPRESS_CHUNK)
            if not data:
                break
            gzip_file.write(data)
    finally:
        fhandle.close()
        gzip_file.close()

    os.rename(filename + '.gz', filename)


def decompress_gzip(filename):
    """Decompress one file "in place" using gzip.

    :param filename: Path to file to decompress.
    :raise: NotFoundError if file is not found.
    """

    tmp_filename = filename + '.tmp'
    tmp_fp = open(tmp_filename, 'wb')

    gz_fp = gzip.GzipFile(filename, 'rb')
    try:
        while True:
            data = gz_fp.read(COMPRESS_CHUNK)
            if not data:
                break
            tmp_fp.write(data)

        # Replace original file with decrypted file.
        os.rename(tmp_filename, filename)
    finally:
        gz_fp.close()
        tmp_fp.close()
