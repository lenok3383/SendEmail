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
    return info_dict, path

def main(path):
    input_info= get_info_from_console()
    info_dict = input_info[0]
    path = input_info[1]
    if not 'host' in info_dict:
        info_dict['host'] = get_config_from_file()
    if not 'msg' in info_dict:
        msg = open(path, 'r').read()
        info_dict['msg'] = msg

class EmailService():

    DEFAULT_PORT = '25'
    SERVICE_READY = r'220'
    SERVICE_NOT_AVAILABLE = r'421'
    CONNECTION_REFUSED = r'Connection refused'
    UNKNOWN_SERV = r'Name or service not known'
    TEL_COMMAND = 'telnet {host} {port}'

    def __init__(self, info_dict):
        self.child = self.establish_connection(info_dict)

    def establish_connection(self, info_dict):
        info_dict['port'] = self.DEFAULT_PORT
        ssh_command = self.TEL_COMMAND.format(**info_dict)

        child = pexpect.spawn(ssh_command)
        expect_options = [self.CONNECTION_REFUSED, self.SERVICE_READY, self.SERVICE_NOT_AVAILABLE,
                          self.UNKNOWN_SERV]

        i = child.expect(expect_options)
        if expect_options[i] == self.CONNECTION_REFUSED:
            child.close(True)
            raise ConnectionRefused(' Unable to connect to remote host: Connection refused')
        elif expect_options[i] == self.SERVICE_NOT_AVAILABLE:
            child.close(True)
            raise NotAvailable('Service not available, closing transmission channel')
        elif expect_options[i] == self.UNKNOWN_SERV:
            child.close(True)
            raise UnknownService('Name or service not known')
        return child

    def send_email(self, info_dict):
        pass

class ConnectionRefused(Exception):
    pass

class NotAvailable(Exception):
    pass

class UnknownService(Exception):
    pass

class TerminationConnection(Exception):
    pass

class RequestedActionAborted(EmailService):
    pass

if __name__ == '__main__':
    main()



