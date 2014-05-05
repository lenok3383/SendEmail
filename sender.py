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


def get_config_from_file(path_conf_file):
    conf_dict = {}
    config = ConfigParser.ConfigParser()
    config.read(path_conf_file)
    server_host = config.get('SectionOne', 'default_smtp')
    conf_dict['server_host'] = server_host
    if 'path_log' in config.options('SectionOne'):
        path_log = config.get('SectionOne', 'path_log')
        conf_dict['path_log'] = path_log
    return conf_dict


def get_info_from_console():
    result = []
    parser = OptionParser()
    parser.add_option("--sender", help="sender email address",
                      dest="sender", type="string")
    parser.add_option("-r", "--recipient", help="email address to "
                       "deliver the message to", dest="recipient",
                      type="string")
    parser.add_option("-s", "--subject", help="subject of message",
                      dest="subject", type="string")
    parser.add_option("--host", help="host", dest="server_host")
    parser.add_option("-p", "--path", help="path to config file",
                      dest="path_conf_file")
    parser.add_option("-m", "--msg", help="path to file with message",
                      dest="msg")
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
    info_dict = {
        'sender': options.sender,
        'recipient': options.recipient,
        'subject': options.subject,
    }
    if options.server_host:
        info_dict['server_host'] = options.server_host
    else:
        if options.path_conf_file:
            info_dict['path_conf_file'] = options.path_conf_file
    info_dict['smtp_port'] = DEFAULT_PORT
    result.append(info_dict)
    if options.msg:
        result.append(options.msg)
    else:
        msg = raw_input("Enter message:")
        info_dict['msg'] = msg
    return result


def main():
    working_directory = os.getcwd()
    DEFAULT_PATH_CONFIG = os.path.join(working_directory,
                                       "config/smtp_config.ini")
    try:
        input_info = get_info_from_console()
    except ValueError, option:
        print 'Try \'python sender.py --help\' for more information.\n', option
        return
    info_dict = input_info[0]
    if not 'server_host' in info_dict:
        if 'path_conf_file' in info_dict:
            config_path = info_dict['path_conf_file']
        else:
            config_path = DEFAULT_PATH_CONFIG
        conf_dict = get_config_from_file(config_path)
        info_dict['server_host'] = conf_dict['server_host']
        if 'path_log' in conf_dict:
                info_dict['path_log'] = conf_dict['path_log']
    if not 'msg' in info_dict:
        msg_path = input_info[1]
        msg = open(msg_path, 'r').read()
        info_dict['msg'] = msg
    try:
        con = EmailService(info_dict)
        result = con.send_email(info_dict)
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
