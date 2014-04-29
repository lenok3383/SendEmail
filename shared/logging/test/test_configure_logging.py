"""Unit tests for configure_logging method.

:Status: $Id: //prod/main/_is/shared/python/logging/test/test_configure_logging.py#2 $
:Authors: vscherb
"""

import logging.config
import shared.logging as sh_logging
import unittest2

from shared.conf import env
from shared.testing.vmock import mockcontrol, matchers

class TestConfigureLogging(unittest2.TestCase):

    def setUp(self):
        self.mc = mockcontrol.MockControl()
        self.file_config_mock = self.mc.mock_method(logging.config, 'fileConfig')
        self.logger_mock = self.mc.mock_class(logging.Logger)
        self.get_logger_mock = self.mc.mock_method(logging, 'getLogger')
        self.is_log_init_mock = self.mc.mock_method(sh_logging,
                                                   'is_logging_initialized')

    def tearDown(self):
        self.mc.tear_down()

    def test_01_configure_logging_from_filepath(self):
        self.is_log_init_mock().returns(False)
        self.file_config_mock('/cool_path/log.conf')
        self.get_logger_mock(matchers.is_str()).returns(self.logger_mock)
        self.logger_mock.debug(matchers.str_with('initialized'))

        self.mc.replay()
        sh_logging.configure_logging('/cool_path/log.conf')
        self.mc.verify()

    def test_02_configure_logging_from_filename(self):
        self.is_log_init_mock().returns(False)
        self.mc.stub_method(env, 'get_conf_root')().returns('/conf_root')
        self.file_config_mock('/conf_root/log.conf')
        self.get_logger_mock(matchers.is_str()).returns(self.logger_mock)
        self.logger_mock.debug(matchers.str_with('initialized'))

        self.mc.replay()
        sh_logging.configure_logging('log.conf')
        self.mc.verify()

    def test_03_logging_reinit_error(self):
        self.is_log_init_mock().returns(True)

        self.mc.replay()
        self.assertRaises(sh_logging.LoggingReinitError,
                          sh_logging.configure_logging,
                          'log.conf')
        self.mc.verify()

if __name__ == '__main__':
    unittest2.main()
