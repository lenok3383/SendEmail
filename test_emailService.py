from unittest import TestCase
from shared.testing.vmock.mockcontrol import MockControl
import pexpect
from exception import ConnectionRefused, NotAvailable, UnknownService,\
                        RequestedActionAborted, TerminationConnection, MySyntaxError
from sending_service import EmailService


class TestEmailService(TestCase):
    SHELL_PROMPT = r':~\$'
    SERVICE_READY = r'220'
    COMPLETED = r'250'
    SERVICE_NOT_AVAILABLE = r'421'
    CONNECTION_REFUSED = r'Connection refused'
    UNKNOWN_SERVICE = r'Name or service not known'
    REQUEST_ABORTED = r'451'
    START_MAIL_INPUT = r'354'
    SERVICE_CLOSING =   r'221'
    SYNTAX_ERROR = r'500'
    CONNECT_TO= r'Connected to'

    COMMAND_CODE_REGEXP = r'(?P<code>\d{3})(?P<other>.+$)'


    def setUp(self):
        self.mc = MockControl()

    def tearDown(self):
        self.mc.tear_down()


    def test_establish_connection(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }
        COMMAND = 'telnet localhost 25'
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.CONNECT_TO,
                           self.UNKNOWN_SERVICE,pexpect.EOF, pexpect.TIMEOUT]).returns(1)
        spawn_mock.expect([self.COMMAND_CODE_REGEXP, pexpect.EOF, pexpect.TIMEOUT]).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.SERVICE_READY)

        self.mc.replay()

        EmailService(test_dict)

        self.mc.verify()

    def test_establish_connection_refused(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }
        COMMAND = "telnet localhost 25"
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.CONNECT_TO,
                           self.UNKNOWN_SERVICE, pexpect.EOF, pexpect.TIMEOUT]).returns(0)
        spawn_mock.close(True)

        self.mc.replay()

        self.assertRaises(ConnectionRefused, EmailService, test_dict)

        self.mc.verify()

    def test_establish_connection_service_not_available(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }
        COMMAND = "telnet localhost 25"
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.CONNECT_TO,
                           self.UNKNOWN_SERVICE, pexpect.EOF, pexpect.TIMEOUT]).returns(1)
        spawn_mock.expect([self.COMMAND_CODE_REGEXP, pexpect.EOF, pexpect.TIMEOUT]).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.SERVICE_NOT_AVAILABLE)
        spawn_mock.close(True)

        self.mc.replay()

        self.assertRaises(NotAvailable, EmailService, test_dict)

        self.mc.verify()

    def test_establish_connection_expect_EOF_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }
        COMMAND = "telnet localhost 25"
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.CONNECT_TO,
                           self.UNKNOWN_SERVICE, pexpect.EOF, pexpect.TIMEOUT]).returns(1)
        spawn_mock.expect([self.COMMAND_CODE_REGEXP, pexpect.EOF, pexpect.TIMEOUT]).returns(1)

        self.mc.replay()

        self.assertRaises(Exception, EmailService, test_dict)

        self.mc.verify()

    def test_establish_connection_expect_TIMEOUT_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }
        COMMAND = "telnet localhost 25"
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.CONNECT_TO,
                           self.UNKNOWN_SERVICE, pexpect.EOF, pexpect.TIMEOUT]).returns(1)
        spawn_mock.expect([self.COMMAND_CODE_REGEXP, pexpect.EOF, pexpect.TIMEOUT]).returns(2)

        self.mc.replay()

        self.assertRaises(Exception, EmailService, test_dict)

        self.mc.verify()

    def test_establish_connection_service_not_known(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'host',
            'msg': 'some text',
            'port': 25
        }
        COMMAND = "telnet host 25"
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.CONNECT_TO,
                           self.UNKNOWN_SERVICE, pexpect.EOF, pexpect.TIMEOUT]).returns(2)
        spawn_mock.close(True)

        self.mc.replay()

        self.assertRaises(UnknownService, EmailService, test_dict)

        self.mc.verify()

    def test_establish_connection_EOF_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }
        COMMAND = 'telnet localhost 25'
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.CONNECT_TO,
                           self.UNKNOWN_SERVICE, pexpect.EOF, pexpect.TIMEOUT]).returns(3)

        self.mc.replay()

        self.assertRaises(Exception, EmailService, test_dict)

        self.mc.verify()

    def test_establish_connection_TIMEOUT_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }
        COMMAND = 'telnet localhost 25'
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.CONNECT_TO,
                           self.UNKNOWN_SERVICE, pexpect.EOF, pexpect.TIMEOUT]).returns(4)

        self.mc.replay()

        self.assertRaises(Exception, EmailService, test_dict)

        self.mc.verify()


    def test_send_email(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('rcpt to: vovaxo@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('DATA')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.START_MAIL_INPUT)
        spawn_mock.sendline('Subject:test letter')
        spawn_mock.sendline('some text\n.')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('quit')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.SERVICE_CLOSING)

        self.mc.replay()

        con = EmailService(test_dict)
        con.send_email(test_dict)

        self.mc.verify()

    def test_send_email_is_not_alive(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(False)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(TerminationConnection, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_request_mail_abroad(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.REQUEST_ABORTED)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(RequestedActionAborted, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_request_mail_another_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns('')

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(Exception, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_request_mail_syntax_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.SYNTAX_ERROR)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(MySyntaxError, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_rcpt_request_aborted(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('rcpt to: vovaxo@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.REQUEST_ABORTED)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(RequestedActionAborted, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_rcpt_request_syntax_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('rcpt to: vovaxo@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.SYNTAX_ERROR)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(MySyntaxError, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_rcpt_another_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value= self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('rcpt to: vovaxo@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns('')

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(Exception, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_data_request_aborted(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('rcpt to: vovaxo@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('DATA')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.REQUEST_ABORTED)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(RequestedActionAborted, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_data_request_syntax_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('rcpt to: vovaxo@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('DATA')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.SYNTAX_ERROR)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(MySyntaxError, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_data_request_another_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('rcpt to: vovaxo@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('DATA')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns('')

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(Exception, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_msg_request_aborted(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('rcpt to: vovaxo@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('DATA')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.START_MAIL_INPUT)
        spawn_mock.sendline('Subject:test letter')
        spawn_mock.sendline('some text\n.')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.REQUEST_ABORTED)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(RequestedActionAborted, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_msg_request_another_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('rcpt to: vovaxo@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('DATA')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.START_MAIL_INPUT)
        spawn_mock.sendline('Subject:test letter')
        spawn_mock.sendline('some text\n.')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns('')

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(Exception, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_quit_syntax_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value = self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('rcpt to: vovaxo@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('DATA')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.START_MAIL_INPUT)
        spawn_mock.sendline('Subject:test letter')
        spawn_mock.sendline('some text\n.')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('quit')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.SYNTAX_ERROR)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(MySyntaxError, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_request_another_error(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text',
            'port': 25
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')
        mock_get_expect_value= self.mc.mock_method(EmailService, 'get_expect_value')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from: lenok@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('rcpt to: vovaxo@gmail.com')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('DATA')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.START_MAIL_INPUT)
        spawn_mock.sendline('Subject:test letter')
        spawn_mock.sendline('some text\n.')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns(self.COMPLETED)
        spawn_mock.sendline('quit')
        spawn_mock.expect(self.COMMAND_CODE_REGEXP).returns(0)
        mock_get_expect_value(spawn_mock).returns('')


        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(Exception, con.send_email, test_dict )

        self.mc.verify()
