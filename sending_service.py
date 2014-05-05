from exception import ConnectionRefusedException, NotAvailableException, UnknownServiceException,\
    RequestedActionAbortedException, TerminationConnectionException, SyntaxErrorException
import pexpect
import logging

class EmailService():
    SERVICE_READY = r'220'
    COMPLETED = r'250'
    SERVICE_NOT_AVAILABLE = r'421'
    CONNECTION_REFUSED = r'Connection refused'
    UNKNOWN_SERVICE = r'Name or service not known'
    REQUEST_ABORTED = r'451'
    START_MAIL_INPUT = r'354'
    SERVICE_CLOSING = r'221'
    SYNTAX_ERROR = r'500'
    CONNECT_TO = r'Connected to'
    TEL_COMMAND = 'telnet {server_host} {smtp_port}'
    MAIL_FROM = 'mail from: {sender}'
    RECIPIENT = 'rcpt to: {recipient}'
    MSG = '{msg}\n.'
    SUBJECT = 'Subject:{subject}'
    COMMAND_CODE_REGEXP = r'(?P<code>\d{3})(?P<other>.+$)'
    SEND_COMPLETED = 'completed'

    def __init__(self, info_dict):
        self.child = self.establish_connection(info_dict)

    def establish_connection(self, info_dict):
        command = self.TEL_COMMAND.format(**info_dict)
        child = pexpect.spawn(command)  # connect to smtp server
        if 'path_log' in info_dict:
            path_log_file = info_dict['path_log']
            try:
                log_output = open(path_log_file, 'w')
                child.logfile = log_output
            except Warning:
                logging.warning('Path to log file is incorrect!')
        expect_options = [self.CONNECTION_REFUSED, self.CONNECT_TO, self.UNKNOWN_SERVICE,
                          pexpect.EOF, pexpect.TIMEOUT]
        smtp_con_option = [self.COMMAND_CODE_REGEXP, pexpect.EOF, pexpect.TIMEOUT]
        i = child.expect(expect_options)
        if expect_options[i] == self.CONNECT_TO:
            k = child.expect(smtp_con_option)
            if smtp_con_option[k] == self.COMMAND_CODE_REGEXP:
                expect_value = self.get_expect_smtp_reply_code(child)
                if expect_value == self.SERVICE_READY:
                    return child
                elif expect_value == self.SERVICE_NOT_AVAILABLE:
                    child.close(True)
                    raise NotAvailableException
            elif smtp_con_option[k] == pexpect.EOF:
                raise Exception('EOF error. SMTP could not connect. Here is what '
                                'SMTP said:', str(child))
            elif smtp_con_option[k] == pexpect.TIMEOUT:
                raise Exception('TIMEOUT error. Here is what SMTP said:', str(child))
        elif expect_options[i] == self.CONNECTION_REFUSED:
            child.close(True)
            raise ConnectionRefusedException
        elif expect_options[i] == self.UNKNOWN_SERVICE:
            child.close(True)
            raise UnknownServiceException
        elif expect_options[i] == pexpect.EOF:
            raise Exception('EOF error. Telnet could not connect. Here is what '
                            'telnet said:', child)
        elif expect_options[i] == pexpect.TIMEOUT:
            raise Exception('TIMEOUT error. Here is what telnet said:', child)

    def get_expect_smtp_reply_code(self, child):
        m = child.match.group('code')
        return m

    def send_email(self, info_dict):
        if not self.child.isalive():        # check is child alive
            raise TerminationConnectionException
        # sending line to smtp server with info about sender
        self.child.sendline(self.MAIL_FROM.format(**info_dict))

        self.child.expect(self.COMMAND_CODE_REGEXP)
         # get SMTP reply code to smtp command MAIL TO
        expect_value = self.get_expect_smtp_reply_code(self.child)
        if expect_value == self.COMPLETED:
            self.child.sendline(self.RECIPIENT.format(**info_dict))
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAbortedException
        elif expect_value == self.SYNTAX_ERROR:
            raise SyntaxErrorException
        else:
            raise Exception('Some another error', expect_value)

        self.child.expect(self.COMMAND_CODE_REGEXP)
         # get SMTP reply code to smtp command RCPT
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
        # get SMTP reply code to smtp command DATA
        expect_value = self.get_expect_smtp_reply_code(self.child)
        if expect_value == self.START_MAIL_INPUT:
            self.child.sendline(self.SUBJECT.format(**info_dict))
            self.child.sendline(self.MSG.format(**info_dict))
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAbortedException
        elif expect_value == self.SYNTAX_ERROR:
            raise SyntaxErrorException
        else:
            raise Exception('Some another error', expect_value)

        self.child.expect(self.COMMAND_CODE_REGEXP)
        # get SMTP reply code to sending message
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
        # get SMTP reply code to smtp command quit
        expect_value = self.get_expect_smtp_reply_code(self.child)
        if expect_value == self.SERVICE_CLOSING:
            return self.SEND_COMPLETED
        if expect_value == self.SYNTAX_ERROR:
            raise SyntaxErrorException
        # if not expect_value == self.SERVICE_CLOSING:
        else:
            raise Exception('Some another error', expect_value)
