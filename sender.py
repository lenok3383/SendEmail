from optparse import OptionParser
import pexpect
import sys
import ConfigParser, os
import re


def get_config_from_file():
    config = ConfigParser.ConfigParser()
    path = "/home/lenok/PyCharmProjects/SendEmail/smtp_config.ini"
    config.read(path)
    host = config.get('SectionOne', 'default_smtp')
    return host


def get_info_from_console():
    info_dict = []
    parser = OptionParser()
    parser.add_option("--sender", type="string", dest="sender")
    parser.add_option("-r", "--recipient", type="string", dest="recipient")
    parser.add_option("-s", "--subject", type="string", dest="subject")
    parser.add_option("--host", dest="host")
    parser.add_option("-p", "--path", dest="path", help="path to file with message")
    (options, args) = parser.parse_args(sys.argv)
    if not options.sender:
        print "Enter sender"
        return
    if not options.recipient:
        print "Enter recipient"
        return
    if not options.subject:
        print "Enter subject"
        return
    info_dict = {
        'sender': options.sender,
        'recipient': options.recipient,
        'subject': options.subject,
        }
    if options.path:
        path = options.path
    msg = raw_input("Enter message:")
    info_dict['msg'] = msg
    if options.host:
        info_dict['host'] = options.host
    result = []
    result.append(info_dict)
    if options.path:
        result.append(options.path)
    return result

def main():
    input_info = get_info_from_console()
    info_dict = input_info[0]
    if not 'host' in info_dict:
        info_dict['host'] = get_config_from_file()
    if not 'msg' in info_dict:
        path = input_info[1]
        msg = open(path, 'r').read()
        info_dict['msg'] = msg
    con = EmailService(info_dict)
    con.send_email(info_dict)


class EmailService():

    DEFAULT_PORT = 25
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

    TEL_COMMAND = 'telnet {host} {port}'
    MAIL_FROM = 'mail from: {sender}'
    RECIPIENT = 'rcpt to: {recipient}'
    MSG = '{subject}\n {msg} \n.'
    COMMAND_CODE_REGEXP = '\d{3}'

    def __init__(self, info_dict):
        self.child = self.establish_connection(info_dict)

    def establish_connection(self, info_dict):
        info_dict['port'] = self.DEFAULT_PORT
        command = self.TEL_COMMAND.format(**info_dict)

        child = pexpect.spawn(command)
        log_output = file('/home/lenok/PyCharmProjects/SendEmail/mylog.txt', 'w')
        child.logfile = log_output

        expect_options = [self.CONNECTION_REFUSED, self.CONNECT_TO, self.UNKNOWN_SERVICE, pexpect.EOF, pexpect.TIMEOUT]
        smtp_con_option = [self.COMMAND_CODE_REGEXP, pexpect.EOF, pexpect.TIMEOUT]

        i = child.expect(expect_options)

        if expect_options[i] == self.CONNECT_TO:
            k = child.expect(smtp_con_option)
            if smtp_con_option[k] == self.COMMAND_CODE_REGEXP:
                expect_value = self.get_group(child)
                if expect_value == self.SERVICE_READY:
                    return child
                elif expect_value == self.SERVICE_NOT_AVAILABLE:
                    child.close(True)
                    raise NotAvailable('Service not available, closing transmission channel')
            elif smtp_con_option[k] == pexpect.EOF:
                raise Exception('EOF error. SMTP could not connect. Here is what SMTP said:',str(child))
            elif smtp_con_option[k] == pexpect.TIMEOUT:
                raise Exception('TIMEOUT error. Here is what SMTP said:',str(child))

        elif expect_options[i] == self.CONNECTION_REFUSED:
            child.close(True)
            raise ConnectionRefused(' Unable to connect to remote host: Connection refused')

        elif expect_options[i] == self.UNKNOWN_SERVICE:
            child.close(True)
            raise UnknownService('Name or service not known')

        elif expect_options[i] == pexpect.EOF:
            raise Exception('EOF error. Telnet could not connect. Here is what telnet said:',child)

        elif expect_options[i] == pexpect.TIMEOUT:
            raise Exception('TIMEOUT error. Here is what telnet said:',child)

    def get_group(self, child):
        m = child.match.group()
        return m

    def send_email(self, info_dict):
        if not self.child.isalive():
            raise TerminationConnection('Connection failed')

        self.child.sendline(self.MAIL_FROM.format(**info_dict))

        self.child.expect(self.COMMAND_CODE_REGEXP)
        expect_value = self.get_group(self.child)
        if expect_value == self.COMPLETED:
            self.child.sendline(self.RECIPIENT.format(**info_dict))
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted('Request action aborted: local error in processing')
        elif expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError('Syntax error, command unrecognised')
        else:
            raise Exception('Some another error', expect_value)

        self.child.expect(self.COMMAND_CODE_REGEXP)
        expect_value = self.get_group(self.child)
        if expect_value == self.COMPLETED:
            self.child.sendline('DATA')
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted('Request action aborted: local error in processing')
        elif expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError('Syntax error, command unrecognised')
        else:
            raise Exception('Some another error', expect_value)

        self.child.expect(self.COMMAND_CODE_REGEXP)
        expect_value = self.get_group(self.child)
        if expect_value == self.START_MAIL_INPUT:
            self.child.sendline(self.MSG.format(**info_dict))
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted('Request action aborted: local error in processing')
        elif expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError('Syntax error, command unrecognised')
        else:
            raise Exception('Some another error',expect_value)

        self.child.expect(self.COMMAND_CODE_REGEXP)
        expect_value = self.get_group(self.child)
        if expect_value == self.COMPLETED:
            self.child.sendline('quit')
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted('Request action aborted: local error in processing')
        elif expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError('Syntax error, command unrecognised')
        else:
            raise Exception('Some another error', expect_value)

        self.child.expect(self.COMMAND_CODE_REGEXP)
        expect_value = self.get_group(self.child)
        if expect_value == self.SERVICE_CLOSING:
            pass
        elif expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError('Syntax error, command unrecognised')
        else:
            raise Exception('Some another error', expect_value)


class ConnectionRefused(Exception):
    pass

class NotAvailable(Exception):
    pass

class UnknownService(Exception):
    pass

class TerminationConnection(Exception):
    pass

class RequestedActionAborted(Exception):
    pass

class MySyntaxError(Exception):
   pass


if __name__ == '__main__':
    main()



