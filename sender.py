from optparse import OptionParser
import pexpect
import sys
import ConfigParser
import os
import re


def get_config_from_file(path):
    config = ConfigParser.ConfigParser()
    config.read(path)
    host = config.get('SectionOne', 'default_smtp')
    return host


def get_info_from_console():
    result = []
    parser = OptionParser()
    parser.add_option("--sender", help="sender email address", dest="sender", type="string")
    parser.add_option("-r", "--recipient", help="email address to deliver the message to", dest="recipient", type="string")
    parser.add_option("-s", "--subject", help="subject of message", dest="subject", type="string")
    parser.add_option("--host", help="host", dest="host")
    parser.add_option("-p", "--path", help="path to config file", dest="path")
    parser.add_option("-m", "--msg", help="path to file with message", dest="msg")
    (options, args) = parser.parse_args(sys.argv)
    missing_options = []
    if not options.sender:
        missing_options.append('sender')
    if not options.recipient:
        missing_options.append('recipient')
    if not options.subject:
        missing_options.append('subject')
    if missing_options:
        raise ValueError('Please specify the following options: %s' % (','.join(missing_options)))
    info_dict = {
        'sender': options.sender,
        'recipient': options.recipient,
        'subject': options.subject,
    }
    if options.host:
        info_dict['host'] = options.host
    else:
        if options.path:
            info_dict['path'] = options.path
    result.append(info_dict)
    if options.msg:
        result.append(options.msg)
    else:
        msg = raw_input("Enter message:")
        info_dict['msg'] = msg
    return result


def main():
    # working_directory = os.getcwd()
    # DEFAULT_PATH_CONFIG = working_directory+"/config/smtp_config.ini"
    try:
        input_info = get_info_from_console()
    except ValueError, option:
        print 'Try \'python sender.py --help\' for more information.\n', option
        return
    info_dict = input_info[0]
    if not 'host' in info_dict:
        if 'path' in info_dict:
            config_path = info_dict['path']
            info_dict['host'] = get_config_from_file(config_path)
        # else:
        #     info_dict['host'] = get_config_from_file(DEFAULT_PATH_CONFIG)
    if not 'msg' in info_dict:
        msg_path = input_info[1]
        msg = open(msg_path, 'r').read()
        info_dict['msg'] = msg
    try:
        con = EmailService(info_dict)
        try:
            con.send_email(info_dict)
        except TerminationConnection:
            print 'Connection failed'
        except RequestedActionAborted:
            print 'Request action aborted: local error in processing'
        except MySyntaxError:
            print 'Syntax error, command unrecognised'
        except Exception, opt:
            print opt
    except ConnectionRefused:
        print ' Unable to connect to remote host: Connection refused'
    except UnknownService:
        print 'Name or service not known'
    except NotAvailable:
        print 'Service not available, closing transmission channel'
    except Exception, opt:
        print opt





class EmailService():
    DEFAULT_PORT = 25
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
    TEL_COMMAND = 'telnet {host} {port}'
    MAIL_FROM = 'mail from: {sender}'
    RECIPIENT = 'rcpt to: {recipient}'
    MSG = '{msg} \n.'
    SUBJECT = 'Subject:{subject}\n'
    COMMAND_CODE_REGEXP = r'(?P<code>\d{3})(?P<other>.+$)'

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
                    raise NotAvailable
            elif smtp_con_option[k] == pexpect.EOF:
                raise Exception('EOF error. SMTP could not connect. Here is what SMTP said:', str(child))
            elif smtp_con_option[k] == pexpect.TIMEOUT:
                raise Exception('TIMEOUT error. Here is what SMTP said:', str(child))
        elif expect_options[i] == self.CONNECTION_REFUSED:
            child.close(True)
            raise ConnectionRefused
        elif expect_options[i] == self.UNKNOWN_SERVICE:
            child.close(True)
            raise UnknownService
        elif expect_options[i] == pexpect.EOF:
            raise Exception('EOF error. Telnet could not connect. Here is what telnet said:', child)
        elif expect_options[i] == pexpect.TIMEOUT:
            raise Exception('TIMEOUT error. Here is what telnet said:', child)

    def get_group(self, child):
        m = child.match.group('code')
        return m

    def send_email(self, info_dict):
        if not self.child.isalive():
            raise TerminationConnection
        self.child.sendline(self.MAIL_FROM.format(**info_dict))
        self.child.expect(self.COMMAND_CODE_REGEXP)
        expect_value = self.get_group(self.child)
        if expect_value == self.COMPLETED:
            self.child.sendline(self.RECIPIENT.format(**info_dict))
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted
        elif expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError
        else:
            raise Exception('Some another error', expect_value)
        self.child.expect(self.COMMAND_CODE_REGEXP)
        expect_value = self.get_group(self.child)
        if expect_value == self.COMPLETED:
            self.child.sendline('DATA')
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted
        elif expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError
        else:
            raise Exception('Some another error', expect_value)
        self.child.expect(self.COMMAND_CODE_REGEXP)
        expect_value = self.get_group(self.child)
        if expect_value == self.START_MAIL_INPUT:
            self.child.sendline(self.SUBJECT.format(**info_dict))
            self.child.sendline(self.MSG.format(**info_dict))
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted
        elif expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError
        else:
            raise Exception('Some another error', expect_value)
        self.child.expect(self.COMMAND_CODE_REGEXP)
        expect_value = self.get_group(self.child)
        if expect_value == self.COMPLETED:
            self.child.sendline('quit')
        elif expect_value == self.REQUEST_ABORTED:
            raise RequestedActionAborted
        elif expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError
        else:
            raise Exception('Some another error', expect_value)
        self.child.expect(self.COMMAND_CODE_REGEXP)
        expect_value = self.get_group(self.child)
        if expect_value == self.SERVICE_CLOSING:
            pass
        elif expect_value == self.SYNTAX_ERROR:
            raise MySyntaxError
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



