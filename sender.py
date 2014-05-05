from optparse import OptionParser
import sys
import ConfigParser
import os
import os.path
from sending_service import EmailService
from exception import ConnectionRefused, NotAvailable, UnknownService, \
            RequestedActionAborted, TerminationConnection, MySyntaxError



def get_config_from_file(path_conf_file):
    conf_dict = {}
    config = ConfigParser.ConfigParser()
    config.read(path_conf_file)
    host = config.get('SectionOne', 'default_smtp')
    conf_dict['host'] = host
    if 'path_log' in config.options('SectionOne'):
        path_log = config.get('SectionOne', 'path_log')
        conf_dict['path_log'] = path_log
    return conf_dict


def get_info_from_console():
    DEFAULT_PORT = 25
    result = []
    parser = OptionParser()
    parser.add_option("--sender", help="sender email address", dest="sender", type="string")
    parser.add_option("-r", "--recipient", help="email address to deliver the message to", dest="recipient", type="string")
    parser.add_option("-s", "--subject", help="subject of message", dest="subject", type="string")
    parser.add_option("--host", help="host", dest="host")
    parser.add_option("-p", "--path", help="path to config file", dest="path_conf_file")
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
        if options.path_conf_file:
            info_dict['path_conf_file'] = options.path_conf_file
    info_dict['port'] = DEFAULT_PORT
    result.append(info_dict)
    if options.msg:
        result.append(options.msg)
    else:
        msg = raw_input("Enter message:")
        info_dict['msg'] = msg
    return result

def main():
    working_directory = os.getcwd()
    DEFAULT_PATH_CONFIG = os.path.join(working_directory+"/config/smtp_config.ini")
    try:
        input_info = get_info_from_console()
    except ValueError, option:
        print 'Try \'python sender.py --help\' for more information.\n', option
        return
    info_dict = input_info[0]
    if not 'host' in info_dict:
        if 'path_conf_file' in info_dict:
            config_path = info_dict['path_conf_file']
        else:
            config_path = DEFAULT_PATH_CONFIG
        conf_dict = get_config_from_file(config_path)
        info_dict['host'] = conf_dict['host']
        if 'path_log' in conf_dict:
                info_dict['path_log'] = conf_dict['path_log']
    if not 'msg' in info_dict:
        msg_path = input_info[1]
        msg = open(msg_path, 'r').read()
        info_dict['msg'] = msg
    try:
        con = EmailService(info_dict)
    except ConnectionRefused:
        print ' Unable to connect to remote host: Connection refused'
    except UnknownService:
        print 'Name or service not known'
    except NotAvailable:
        print 'Service not available, closing transmission channel'
    except Exception, opt:
        print opt
    try:
        con = EmailService(info_dict)
        con.send_email(info_dict)
    except TerminationConnection:
        print 'Connection failed'
    except RequestedActionAborted:
        print 'Request action aborted: local error in processing'
    except MySyntaxError:
        print 'Syntax error, command unrecognised'
    except Exception, opt:
        print opt

if __name__ == '__main__':
    main()



