"""
MD5 Utility Helpers.

Coro functionality has been removed temporarily and will be added
when the first product using it is ported to python 2.6.

:Author: ivlesnik
:Last Modified By: $Author: vkuznets $

$Id: //prod/main/_is/shared/python/file/md5.py#3 $
"""

import hashlib


CHECKSUM_CHUNK_SIZE = 2 ** 20 # 1MB


class MD5MismatchError(Exception):

    """
    Exception class to hold info about an md5 mismatch.
    """

    def __init__(self, msg, in_md5=None, expected_md5=None):
        """
        :param msg: error message to pass to an exception instance.
        :param in_md5: what you got...
        :param expected_md5: what you wanted...
        """

        message = '%s: got %s, expected %s' % (msg, in_md5, expected_md5)
        Exception.__init__(self, message)
        self.msg = message

def verify_md5(file_path, correct_md5):
    """Verify the given file path against a provided (hex-digested) md5 string.

    :param file_path: path to a file to be verified.

    :raises MD5MismatchError: if the verification fails.

    :return: None
    """

    unverified_file_md5 = compute_file_md5(file_path).hexdigest()

    if unverified_file_md5 != correct_md5:
        raise MD5MismatchError('Failed MD5 Check',
            unverified_file_md5, correct_md5)

def check_file_md5(file_path, md5_file_path):
    """Check the MD5 of the subject_path given an MD5 in the md5_path.

    Given a path to the 'subject' file to be verified and a path to the file
    containing an md5 hexdigest sum, compare the md5 of the 'subject' file
    to the given expected checksum.

    :param file_path: path to file to be verified.

    :raises MD5MismatchError: if the MD5 digests comparison fails.

    :return: MD5 hexdigest of file_path, contained in md5_file_path.
    """

    with open(md5_file_path) as _file:
        md5_checksum = _file.read().strip()
        verify_md5(file_path, md5_checksum)

    return md5_checksum

def compute_file_md5(src_file_path):
    """Compute the checksum of a file.

    :param src_file_path: path to file to calculate md5 checksum for it.

    :return: hashlib object, as created by hashlib.md5(),
             populated with the file content hash.
    """

    checksum = hashlib.md5()
    with open(src_file_path) as _file:
        while True:
            chunk = _file.read(CHECKSUM_CHUNK_SIZE)
            if not chunk:
                break
            checksum.update(chunk)

    return checksum
