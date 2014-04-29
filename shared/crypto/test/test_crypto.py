"""Unit tests for shared.scm.perforce module.

:Status: $Id: //prod/main/_is/shared/python/crypto/test/test_crypto.py#2 $
:Author: ivlesnik
:Last Modified By: $Author: mikmeyer $
"""

import os
import shutil
import tempfile
import unittest2

from shared.crypto import crypto


class TestCrypto(unittest2.TestCase):
    """Test Crypto module."""

    def setUp(self):
        """Setup logic for each unit test."""

    def test_01_des3_cbc_random_iv_and_key(self):
        """Test des3_cbc_random_iv_and_key function."""

        # Check result sizes.
        vector, key = crypto.des3_cbc_random_iv_and_key()
        self.assertEqual(len(vector), 8)
        self.assertEqual(len(key), 16)

        # Check randomization.
        vector2, key2 = crypto.des3_cbc_random_iv_and_key()
        self.assertNotEqual((vector, key), (vector2, key2))

    def test_02_encrypt_decrypt_file(self):
        """Test des3_cbc_encrypt_file and des3_cbc_decrypt_file functions."""

        test_string = 'This is a plain text string.'
        temp_dir = tempfile.mkdtemp(prefix='crypto_')
        original_file = os.path.join(temp_dir, 'original_file')
        encrypted_file = os.path.join(temp_dir, 'encrypted_file')
        decrypted_file = os.path.join(temp_dir, 'decrypted_file')
        vector, key = 'dn864hdt', 's8JK965Hngty7620'

        # Create original test file.
        with open(original_file, 'w') as fp:
            fp.write(test_string)

        # Check des3_cbc_encrypt_file function.
        expected_result = ('646e383634686474',
            '73384a4b393635486e67747937363230', 8,
            '3dc33289003972e8abb5c5c9c9a5cc87')
        actual_result = crypto.des3_cbc_encrypt_file(original_file,
            encrypted_file, vector, key)
        self.assertEqual(expected_result, actual_result)

        # The encrypted file should exist.
        self.assertTrue(os.path.exists(encrypted_file))

        # Check des3_cbc_decrypt_file function.
        expected_result = ('646e383634686474',
            '73384a4b393635486e67747937363230', 8,
            'd7270ec023e20d6fc6a8dd828d015b6d')
        actual_result = crypto.des3_cbc_decrypt_file(encrypted_file,
            decrypted_file, vector, key)
        self.assertEqual(expected_result, actual_result)

        # The decrypted file should exist.
        self.assertTrue(os.path.exists(decrypted_file))

        # Compare decrypted file contents to the original one.
        with open(decrypted_file) as fp:
            self.assertEqual(fp.read(), test_string)

        # Clean up temporary space.
        shutil.rmtree(temp_dir)

if __name__ == '__main__':

    unittest2.main()
