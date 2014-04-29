"""Unit tests for backdoor.py

:Status: $Id: //prod/main/_is/shared/python/process/test/test_backdoor.py#7 $
:Author: scottrwi
"""

import logging
import os
import random
import socket
import threading
import time

import unittest2 as unittest

from shared.process import backdoor

# Setup logging
LOG_LEVEL = logging.CRITICAL
logging.basicConfig(level=LOG_LEVEL)

WELCOME_MESSAGE = 'Test welcome message'
LOCALS = {'foo': 'foo_value'}

class TestBackdoor(unittest.TestCase):

    def setUp(self):

        address = ('127.0.0.1', 48133)
        self.server = backdoor.BackdoorServer(address,
                                       welcome_message=WELCOME_MESSAGE,
                                       local_vars=LOCALS)

        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

        self.cli_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.cli_sock.connect(address)
        self.cli_sock.settimeout(0.1)

    def get_resp(self):
        resp = ''
        while True:
            try:
                curr = self.cli_sock.recv(1024)
                if not curr:
                    break
                resp += curr
            except socket.timeout:
                break
        return resp

    def test_welcome(self):
        resp = self.get_resp()
        self.assertTrue(resp.startswith(WELCOME_MESSAGE))

    def test_locals(self):
        self.cli_sock.send('foo\n')
        resp = self.get_resp()
        self.assertTrue(resp.find(LOCALS['foo']))

    def test_error(self):
        self.cli_sock.send('unknown_name\n')
        resp = self.get_resp()
        self.assertGreater(resp.find('NameError'), -1)

    def test_eof(self):
        self.get_resp()
        self.cli_sock.send('\004')
        resp = self.get_resp()
        # Sends a newline but no prompt.
        self.assertEqual(resp, '\n')

    def tearDown(self):
        self.cli_sock.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


if __name__  == '__main__':
    unittest.main()
