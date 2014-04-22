from unittest import TestCase
from shared.testing.vmock.mockcontrol import MockControl
import pexpect
from sender import EmailService, ConnectionRefused, NotAvailable, UnknownService, \
            RequestedActionAborted, TerminationConnection


class TestEmailService(TestCase):
    SHELL_PROMPT = r':~\$'
    SERVICE_READY = r'220'
    COMPLETED = r'250'
    SERVICE_NOT_AVAILABLE = r'421'
    CONNECTION_REFUSED = r'Connection refused'
    UNKNOWN_SERVICE = r'Name or service not known'
    ERROR = r'4'
    START_MAIL_INPUT = r'354'
    SERVICE_CLOSING =   r'221'
    SYNTAX_ERROR = r'500'


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
            'msg': 'some text'
        }
        COMMAND = 'telnet localhost 25'
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.SERVICE_READY,
                           self.SERVICE_NOT_AVAILABLE, self.UNKNOWN_SERVICE]).returns(1)

        self.mc.replay()

        EmailService(test_dict)

        self.mc.verify()

    def test_establish_connection_refused(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text'
        }
        COMMAND = "telnet localhost 25"
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.SERVICE_READY,
                           self.SERVICE_NOT_AVAILABLE, self.UNKNOWN_SERVICE]).returns(0)
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
            'msg': 'some text'
        }
        COMMAND = "telnet localhost 25"
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.SERVICE_READY,
                           self.SERVICE_NOT_AVAILABLE, self.UNKNOWN_SERVICE]).returns(2)
        spawn_mock.close(True)

        self.mc.replay()

        self.assertRaises(NotAvailable, EmailService, test_dict)

        self.mc.verify()

    def test_establish_connection_service_not_known(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'host',
            'msg': 'some text'
        }
        COMMAND = "telnet host 25"
        spawn_mock = self.mc.mock_class(pexpect.spawn)
        spawn_ctor_mock = self.mc.mock_constructor(pexpect, 'spawn')

        spawn_ctor_mock(COMMAND).returns(spawn_mock)

        spawn_mock.expect([self.CONNECTION_REFUSED, self.SERVICE_READY,
                           self.SERVICE_NOT_AVAILABLE, self.UNKNOWN_SERVICE]).returns(3)
        spawn_mock.close(True)

        self.mc.replay()

        self.assertRaises(UnknownService, EmailService, test_dict)

        self.mc.verify()


    def test_send_email(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text'
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from:<lenok@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(0)
        spawn_mock.sendline('rcpt to:<vovaxo@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(0)
        spawn_mock.sendline('DATA')
        spawn_mock.expect([self.START_MAIL_INPUT, self.ERROR]).returns(0)
        spawn_mock.sendline('some text and .')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(0)
        spawn_mock.sendline('quit')
        spawn_mock.expect([self.SERVICE_CLOSING, self.SYNTAX_ERROR]).returns(0)

        self.mc.replay()

        con = EmailService(test_dict)
        result = con.send_email(test_dict)

        self.mc.verify()

    def test_send_email_is_not_alive(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text'
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
            'msg': 'some text'
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from:<lenok@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(1)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(RequestedActionAborted, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email_request_mail_abroad(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text'
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from:<lenok@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(1)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(RequestedActionAborted, con.send_email, test_dict )

        self.mc.verify()


    def test_send_email(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text'
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from:<lenok@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(0)
        spawn_mock.sendline('rcpt to:<vovaxo@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(1)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(RequestedActionAborted, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text'
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from:<lenok@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(0)
        spawn_mock.sendline('rcpt to:<vovaxo@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(0)
        spawn_mock.sendline('DATA')
        spawn_mock.expect([self.START_MAIL_INPUT, self.ERROR]).returns(1)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(RequestedActionAborted, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text'
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from:<lenok@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(0)
        spawn_mock.sendline('rcpt to:<vovaxo@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(0)
        spawn_mock.sendline('DATA')
        spawn_mock.expect([self.START_MAIL_INPUT, self.ERROR]).returns(0)
        spawn_mock.sendline('some text and .')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(1)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(RequestedActionAborted, con.send_email, test_dict )

        self.mc.verify()

    def test_send_email(self):
        test_dict = {
            'sender': 'lenok@gmail.com',
            'recipient': 'vovaxo@gmail.com',
            'subject': 'test letter',
            'host': 'localhost',
            'msg': 'some text'
        }

        spawn_mock = self.mc.mock_class(pexpect.spawn)
        mock_establish_connection = self.mc.mock_method(EmailService, 'establish_connection')

        mock_establish_connection(test_dict).returns(spawn_mock)

        spawn_mock.isalive().returns(True)
        spawn_mock.sendline('mail from:<lenok@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(0)
        spawn_mock.sendline('rcpt to:<vovaxo@gmail.com>')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(0)
        spawn_mock.sendline('DATA')
        spawn_mock.expect([self.START_MAIL_INPUT, self.ERROR]).returns(0)
        spawn_mock.sendline('some text and .')
        spawn_mock.expect([self.COMPLETED, self.ERROR]).returns(0)
        spawn_mock.sendline('quit')
        spawn_mock.expect([self.SERVICE_CLOSING, self.SYNTAX_ERROR]).returns(1)

        self.mc.replay()

        con = EmailService(test_dict)
        self.assertRaises(SyntaxError, con.send_email, test_dict )

        self.mc.verify()



