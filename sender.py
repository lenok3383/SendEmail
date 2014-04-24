from optparse import OptionParser
import pexpect
import sys
import ConfigParser, os


def get_config_from_file(self):
    config = ConfigParser.ConfigParser()
    path = "/home/lenok/PyCharmProjects/SendEmail/smtp_config.ini"
    # path = os.getcwd()
    config.read(path)
    host = config.get('SectionOne', 'default_smtp')
    return host


def get_info_from_console(self):
    global path
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
        self.path = options.path
    msg = raw_input("Enter message:")
    info_dict['msg'] = msg
    if options.host:
        info_dict['host'] = options.host
    return info_dict, path

def main():
    input_info = get_info_from_console()
    info_dict = input_info[0]
    path = input_info[1]
    if not 'host' in info_dict:
        info_dict['host'] = get_config_from_file()
    if not 'msg' in info_dict:
        msg = open(path, 'r').read()
        info_dict['msg'] = msg

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
    MSG = '{subject}\n {msg} \n.\n'
    COMMAND_CODE_REGEXP = '^\d{3}'

    def __init__(self, info_dict):
        self.child = self.establish_connection(info_dict)

    def establish_connection(self, info_dict):
        info_dict['port'] = self.DEFAULT_PORT
        command = self.TEL_COMMAND.format(**info_dict)

        child = pexpect.spawn(command)

        expect_options = [self.CONNECTION_REFUSED, self.CONNECT_TO, self.UNKNOWN_SERVICE, '']

        i = child.expect(expect_options)

        if expect_options[i] == self.CONNECT_TO:
            child.expect(self.COMMAND_CODE_REGEXP)
            self.expect_value = self.get_group()
            if self.expect_value == self.SERVICE_READY:
                return child
            elif self.expect_value == self.SERVICE_NOT_AVAILABLE:
                child.close(True)
                raise NotAvailable('Service not available, closing transmission channel')
            else:
                child.close(True)
                raise Exception('Some another error')

        elif expect_options[i] == self.CONNECTION_REFUSED:
            child.close(True)
            raise ConnectionRefused(' Unable to connect to remote host: Connection refused')

        elif expect_options[i] == self.UNKNOWN_SERVICE:
            child.close(True)
            raise UnknownService('Name or service not known')
        else:
            child.close(True)
            raise Exception('Some another error')

    def get_group(self):
        self.m = self.child.match()
        return self.m.group()

#     SERVICE_READY = r'220'
#     COMPLETED = r'250'
#     SERVICE_NOT_AVAILABLE = r'421'
#     CONNECTION_REFUSED = r'Connection refused'
#     UNKNOWN_SERVICE = r'Name or service not known'
#     REQUEST_ABORTED = r'451' # 451 request action aborted
#     START_MAIL_INPUT = r'354'
#     SERVICE_CLOSING =   r'221'
#     SYNTAX_ERROR = r'500'
#     CONNECT_TO= r'Connected to'
#     COMMAND_CODE_REGEXP = '^\d{3}'

    def send_email(self, info_dict):
        if not self.child.isalive():
            raise TerminationConnection('Connection failed')

        self.child.sendline(self.MAIL_FROM.format(**info_dict))

        self.child.expect(self.COMMAND_CODE_REGEXP)
        self.expect_value = self.get_group()
        if self.expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted('Request action aborted: local error in processing')
        elif self.expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError('Syntax error, command unrecognised')
        elif self.expect_value == self.COMPLETED:
            self.child.sendline(self.RECIPIENT.format(**info_dict))
        else:
            raise Exception('Some another error')

        self.child.expect(self.COMMAND_CODE_REGEXP)
        self.expect_value = self.get_group()
        if self.expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted('Request action aborted: local error in processing')
        elif self.expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError('Syntax error, command unrecognised')
        elif self.expect_value == self.COMPLETED:
            self.child.sendline('DATA')
        else:
            raise Exception('Some another error')

        self.child.expect(self.COMMAND_CODE_REGEXP)
        self.expect_value = self.get_group()
        if self.expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted('Request action aborted: local error in processing')
        elif self.expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError('Syntax error, command unrecognised')
        elif self.expect_value == self.START_MAIL_INPUT:
            self.child.sendline(self.MSG.format(**info_dict))
        else:
            raise Exception('Some another error')

        self.child.expect(self.COMMAND_CODE_REGEXP)
        self.expect_value = self.get_group()
        if self.expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted('Request action aborted: local error in processing')
        elif self.expect_value == self.COMPLETED:
            self.child.sendline('quit')
        else:
            raise Exception('Some another error')

        self.child.expect(self.COMMAND_CODE_REGEXP)
        self.expect_value = self.get_group()
        if self.expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError('Syntax error, command unrecognised')
        elif not self.expect_value == self.SERVICE_CLOSING:
            raise Exception('Some another error')


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



