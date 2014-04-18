from optparse import OptionParser
import pexpect
import sys
import ConfigParser, os


def get_config_from_file(self, path):
    config = ConfigParser.ConfigParser()
    with open(config.cfg) as f:
        config.read(f)
    host = config.get(path, 'default_smtp')


def get_info_from_console(self):
    info_dict = []
    parser = OptionParser()
    parser.add_option("--sender", type="string", dest="sender")
    parser.add_option("-r", "--recipient", type="string", dest="recipient")
    parser.add_option("-s", "--subject", type="string", dest="subject")
    parser.add_option("--host", dest="host")
    parser.add_option("-p", "--path", dest="path")
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
    if not options.path:
        msg = raw_input("Enter message:")
        info_dict['msg'] = msg
    info_dict = {
        'sender': options.sender,
        'recipient': options.recipient,
        'subject': options.subject,
        'host': options.host
        }
    path = options.path
    return info_dict

def main(path):
    get_config_from_file()
    info_dict = get_info_from_console()
    if not 'msg' in info_dict:
        msg = open(path, 'r').read()
        info_dict['msg'] = msg

def send_email(self):
    DEFAULT_PORT = 25
    pass


if __name__ == '__main__':
    main()



