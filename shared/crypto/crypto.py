"""Crypto utilities.  This module requires the ``pycrypto`` package.

:Status: $Id: //prod/main/_is/shared/python/crypto/crypto.py#2 $
:Authors: jwescott
:Last Modified By: $Author: mikmeyer $
"""

import binascii
import hashlib
import os

from Crypto.Cipher import DES3

FILE_BUFFER_SIZE = 65536


def des3_cbc_random_vector_and_key():
    """Generate random vector and key.

    :Parameters:
        None
    :Return: tuple (vector, key),
        where vector is initialization vector and key is random key.
    """

    checksum = hashlib.md5()
    for index in xrange(50):
        checksum.update(os.urandom(16))
    key = binascii.unhexlify(checksum.hexdigest())

    # Generate initialization vector (needs to be DES3.block_size bytes).
    for index in xrange(50):
        checksum.update(os.urandom(16))
    vector = binascii.unhexlify(checksum.hexdigest()[:DES3.block_size * 2])

    return (vector, key)


def des3_cbc_encrypt_file(src, dest, vector, key):
    """Encrypt file using DES3 in CBC mode.

    Use `vector` as an initialization vector and `key` as a key.

    :Parameters:
        `src`: source filename.
        `dest`: destination filename.
        `vector`: initialization vector.
        `key`: the encryption key.
    :Returns: tuple (vector, key, block_size, md5sum of encrypted file)
    """

    md5sum = hashlib.md5()
    with open(src) as src_fp:
        with open(dest, 'w') as dest_fp:
            cipher = DES3.new(key, DES3.MODE_CBC, vector)
            done = False
            while not done:
                data = src_fp.read(FILE_BUFFER_SIZE)
                if len(data) < FILE_BUFFER_SIZE:
                    # Add padding bytes.  The file is padded with bytes
                    # all of the same value as the number of padding bytes
                    # (as per PKCS5, RFC2630, and NIST 800-38a).
                    padding_len = DES3.block_size - len(data) % \
                        DES3.block_size
                    data += binascii.unhexlify(('0' + str(padding_len)) *
                                               padding_len)
                    done = True
                encrypted_data = cipher.encrypt(data)
                dest_fp.write(encrypted_data)
                md5sum.update(encrypted_data)

    key = binascii.hexlify(key)
    vector = binascii.hexlify(vector)
    return (vector, key, DES3.block_size, md5sum.hexdigest())


def des3_cbc_decrypt_file(src, dest, vector, key):
    """Decrypt file using DES3 in CBC mode.

    Use `vector` as an initialization vector and `key` as a key.

    :Parameters:
        `src`: source filename.
        `dest`: destination filename.
        `vector`: initialization vector.
        `key`: the encryption key.

    :Returns: tuple (vector, key, block_size, md5sum of decrypted file)
    """

    md5sum = hashlib.md5()
    with open(src) as src_fp:
        with open(dest, 'w') as dest_fp:
            cipher = DES3.new(key, DES3.MODE_CBC, vector)
            data = src_fp.read(FILE_BUFFER_SIZE)
            while data:
                decrypted_data = cipher.decrypt(data)
                data = src_fp.read(FILE_BUFFER_SIZE)
                if not data:
                    # Remove padding bytes. The file is padded with bytes
                    # all of the same value as the number of padding bytes
                    # (as per PKCS5, RFC2630, and NIST 800-38a).
                    padding_len = int(binascii.hexlify(decrypted_data[-1]))
                    decrypted_data = decrypted_data[:-(padding_len)]
                dest_fp.write(decrypted_data)
                md5sum.update(decrypted_data)

    key = binascii.hexlify(key)
    vector = binascii.hexlify(vector)

    return (vector, key, DES3.block_size, md5sum.hexdigest())
