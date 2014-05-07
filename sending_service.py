from exception import ConnectionRefusedException, NotAvailableException,\
    UnknownServiceException, RequestedActionAbortedException, \
    TerminationConnectionException, SyntaxErrorException
import pexpect
import logging


class EmailService():
    SERVICE_READY = '220'
    COMPLETED = '250'
    SERVICE_NOT_AVAILABLE = '421'
    CONNECTION_REFUSED = 'Connection refused'
    UNKNOWN_SERVICE = 'Name or service not known'
    REQUEST_ABORTED = '451'
    START_MAIL_INPUT = '354'
    SERVICE_CLOSING = '221'
    SYNTAX_ERROR = '500'

    TEL_COMMAND = 'telnet {host} {port}'
    MAIL_FROM = 'mail from: {sender}'
    RECIPIENT = 'rcpt to: {recipient}'
    MSG = '{msg}\n.'
    SUBJECT = 'Subject:{subject}'
    COMMAND_CODE_REGEXP = '(?P<code>\d{3})(?P<other>.+$)'
    SEND_COMPLETED = 'completed'
    CONNECT = 'Connected to {host}'

    def __init__(self, smtp_host, smtp_port, log_path):
        self.child = self.establish_connection(smtp_host,
                                                log_path, smtp_port)

    def establish_connection(self, smtp_host, log_path, smtp_port):
        CONNECT_TO = self.CONNECT.format(host=smtp_host)

        command = self.TEL_COMMAND.format(host=smtp_host, port=smtp_port)
        child = pexpect.spawn(command)  # connect to smtp server

        if log_path != '':
            try:
                log_output = open(log_path, 'w')
                child.logfile = log_output
            except IOError, opt:
                logging.basicConfig(level=logging.DEBUG)
                logging.warning(u'Failed to open pexpect log file: %s' % opt)

        expect_options = [self.CONNECTION_REFUSED, CONNECT_TO,
                          self.UNKNOWN_SERVICE, pexpect.EOF, pexpect.TIMEOUT]
        smtp_con_option = [self.COMMAND_CODE_REGEXP, pexpect.EOF,
                           pexpect.TIMEOUT]

        i = child.expect(expect_options)
        if expect_options[i] == CONNECT_TO:
            k = child.expect(smtp_con_option)

            if smtp_con_option[k] == self.COMMAND_CODE_REGEXP:
                expect_value = self.get_expect_smtp_reply_code(child)
                if expect_value == self.SERVICE_READY:
                    return child
                elif expect_value == self.SERVICE_NOT_AVAILABLE:
                    child.close(True)
                    raise NotAvailableException
            elif smtp_con_option[k] ==  pexpect.EOF:
                raise Exception('EOF error.SMTP could not connect.'
                                ' Here is what SMTP said:', child.before)
            elif smtp_con_option[k] == pexpect.TIMEOUT:
                raise Exception('TIMEOUT error. Here is what SMTP said:',
                                child.before)

        elif expect_options[i] == self.CONNECTION_REFUSED:
            child.close(True)
            raise ConnectionRefusedException
        elif expect_options[i] == self.UNKNOWN_SERVICE:
            child.close(True)
            raise UnknownServiceException
        elif expect_options[i] == pexpect.EOF:
            raise Exception('EOF error. Telnet could not connect.'
                            ' Here is what telnet said:', str(child.before))
        elif expect_options[i] == pexpect.TIMEOUT:
            raise Exception('TIMEOUT error. Here is what telnet said:',
                            str(child.before))

    def get_expect_smtp_reply_code(self, child):
        m = child.match.group('code')
        return m

    def send_email(self, sender, recipient, subject, msg):
        if not self.child.isalive():  # check is child alive
            raise TerminationConnectionException
        # sending line to smtp server with info about sender
        self.child.sendline(self.MAIL_FROM.format(sender=sender))

        self.child.expect(self.COMMAND_CODE_REGEXP)
        # get answer (SMTP reply code) from smtp command MAIL TO
        expect_value = self.get_expect_smtp_reply_code(self.child)
        if expect_value == self.COMPLETED:
            self.child.sendline(self.RECIPIENT.format(recipient=recipient))
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAbortedException
        elif expect_value == self.SYNTAX_ERROR:
            raise SyntaxErrorException
        else:
            raise Exception('Some another error', expect_value)

        self.child.expect(self.COMMAND_CODE_REGEXP)
        # get answer (SMTP reply code) from smtp command RCPT
        expect_value = self.get_expect_smtp_reply_code(self.child)
        if expect_value == self.COMPLETED:
            self.child.sendline('DATA')
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAbortedException
        elif expect_value == self.SYNTAX_ERROR:
            raise SyntaxErrorException
        else:
            raise Exception('Some another error', expect_value)

        self.child.expect(self.COMMAND_CODE_REGEXP)
        # get answer (SMTP reply code) from smtp command DATA
        expect_value = self.get_expect_smtp_reply_code(self.child)
        if expect_value == self.START_MAIL_INPUT:
            self.child.sendline(self.SUBJECT.format(subject=subject))
            self.child.sendline(self.MSG.format(msg=msg))
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAbortedException
        elif expect_value == self.SYNTAX_ERROR:
            raise SyntaxErrorException
        else:
            raise Exception('Some another error', expect_value)

        self.child.expect(self.COMMAND_CODE_REGEXP)
        # get answer (SMTP reply code)  from sending message
        expect_value = self.get_expect_smtp_reply_code(self.child)
        if expect_value == self.COMPLETED:
            self.child.sendline('quit')
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAbortedException
        elif expect_value == self.SYNTAX_ERROR:
            raise SyntaxErrorException
        else:
            raise Exception('Some another error', expect_value)

        self.child.expect(self.COMMAND_CODE_REGEXP)
        # get answer (SMTP reply code) from smtp command quit
        expect_value = self.get_expect_smtp_reply_code(self.child)
        if expect_value == self.SERVICE_CLOSING:
            return self.SEND_COMPLETED
        if expect_value == self.SYNTAX_ERROR:
            raise SyntaxErrorException
        # if not expect_value == self.SERVICE_CLOSING:
        else:
            raise Exception('Some another error', expect_value)
