from optparse import OptionParser
import sys
import ConfigParser
import os
from sending_service import EmailService
from exception import ConnectionRefusedException, NotAvailableException,\
    UnknownServiceException, RequestedActionAbortedException,\
    TerminationConnectionException, SyntaxErrorException


DEFAULT_PORT = 25
SEND_COMPLETED = 'completed'


def get_config_from_file(conf_file_path):
    conf_dict = {}
    config = ConfigParser.ConfigParser()
    config.read(conf_file_path)
    if 'default_smtp' in config.options('SectionOne'):
        smtp_host = config.get('SectionOne', 'default_smtp')
        conf_dict['smtp_host'] = smtp_host
    if 'log_path' in config.options('SectionOne'):
        log_path = config.get('SectionOne', 'log_path')
        conf_dict['log_path'] = log_path
    return conf_dict


def get_info_from_console():
    parser = OptionParser()
    parser.add_option("--sender", help="sender email address",
                      dest="sender", type="string")
    parser.add_option("-r", "--recipient", help="email address to "
                       "deliver the message to", dest="recipient",
                      type="string")
    parser.add_option("-s", "--subject", help="subject of message",
                      dest="subject", type="string")
    parser.add_option("--host", help="host", dest="smtp_host")
    parser.add_option("-p", "--path", help="path to config file",
                      dest="conf_file_path")
    parser.add_option("-m", "--msg", help="path to file with message",
                      dest="msg_path")
    (options, args) = parser.parse_args(sys.argv)
    missing_options = []
    if not options.sender:
        missing_options.append('sender')
    if not options.recipient:
        missing_options.append('recipient')
    if not options.subject:
        missing_options.append('subject')
    if missing_options:
        raise ValueError('Please specify the following options: %s'
                         % (','.join(missing_options)))
    console_options = {
        'sender': options.sender,
        'recipient': options.recipient,
        'subject': options.subject,
    }
    if options.smtp_host:
        console_options['smtp_host'] = options.smtp_host
    if options.conf_file_path:
        console_options['conf_file_path'] = options.conf_file_path
    if options.msg_path:
        console_options['msg_path'] = options.msg_path
    else:
        msg = raw_input("Enter message:")
        console_options['msg'] = msg
    return console_options


def main():
    DEFAULT_PATH_CONFIG = os.path.join("config/smtp_config.ini")
    try:
        console_options = get_info_from_console()
    except ValueError, option:
        print 'Try \'python sender.py --help\' for more information.\n', option
        return

    config_path = console_options.get('conf_file_path', DEFAULT_PATH_CONFIG)
    conf_dict = get_config_from_file(config_path)
    conf_dict.update(console_options)

    if not 'msg' in conf_dict:
        msg_path = console_options['msg_path']
        file = open(msg_path, 'r')
        try:
            msg = file.read()
            conf_dict['msg'] = msg
        finally:
            file.close()

    log_path = conf_dict.get('log_path', '')
    smtp_host = conf_dict['smtp_host']
    smtp_port = DEFAULT_PORT
    sender = conf_dict['sender']
    recipient = conf_dict['recipient']
    subject = conf_dict['subject']
    msg = conf_dict['msg']

    try:
        con = EmailService(smtp_host, smtp_port, log_path)
        result = con.send_email(sender, recipient, subject, msg)
        if result == SEND_COMPLETED:
            print 'Send mail action okay, completed'
    except ConnectionRefusedException:
        print ' Unable to connect to remote host: Connection refused'
    except UnknownServiceException:
        print 'Name or service not known'
    except NotAvailableException:
        print 'Service not available, closing transmission channel'
    except TerminationConnectionException:
        print 'Connection failed'
    except RequestedActionAbortedException:
        print 'Request action aborted: local error in processing'
    except SyntaxErrorException:
        print 'Syntax error, command unrecognised'
    except Exception, opt:
        print opt


if __name__ == '__main__':
        main()
