"""Client for SPAMD (IronPort) Protocol

It is is designed to work with the custom IronPort spamd server as well as the
upstream spamd.

:Status: $Id: //prod/main/_is/shared/python/client/spamd.py#1 $
:Author: ddrawhor
"""

import re
import socket
from cBencode import bencode, bdecode

PROTOCOL_VERSION = '1.4'

class SpamdResponse(object):

    """Class to store the returned spamd response"""

    __slots__ = ('decoded_case', 'email_msg', 'ipas_score', 'is_spam',
                 'msg_content_length', 'resp_code', 'resp_msg', 'score',
                 'threshold', 'version')

    def __init__(self):
        self.decoded_case = None
        self.email_msg = ''
        self.ipas_score = None
        self.is_spam = None
        self.msg_content_length = 0
        self.resp_code = None
        self.resp_msg = None
        self.score = None
        self.threshold = None
        self.version = None

    def __str__(self):
        """Return the entire SpamdResponse content as a string"""

        ret_list = []
        ret_list.append('response: %s : %s\n'
                        % (self.resp_code, self.resp_msg,))
        ret_list.append('spamd version: %s\n' % (self.version,))
        ret_list.append('is_spam: %s\n' % (self.is_spam,))
        ret_list.append('score: %s\n' % (self.score,))
        ret_list.append('threshold: %s\n' % (self.threshold,))
        ret_list.append('ipas_score: %s\n' % (self.ipas_score,))
        ret_list.append('case: %s\n' % (self.decoded_case,))
        if self.msg_content_length > 0:
            ret_list.append('length: %s\n' % (self.msg_content_length,))
            ret_list.append('EMAIL:\n\n')
            ret_list.append(self.email_msg)

        return ''.join(ret_list)


def lines_from_sock(sock):
    """Generator for reading buffered lines from the socket"""

    unconsumed = ''
    while True:
        recv_buffer = sock.recv(4096)
        if not recv_buffer:
            yield unconsumed
            break

        unconsumed += recv_buffer
        lines = unconsumed.split('\r\n')
        for line in lines[0:-1]:
            yield line
        unconsumed = lines[-1]


class SpamdClientError(Exception):
    pass


class SpamdClientMalformedRPC(SpamdClientError):
    pass


class SpamdClientMissingRPC(SpamdClientError):
    pass


class SpamdClientProtocolError(SpamdClientError):
    pass


# SPAMD/1.4 0 EX_OK
response_header_re = re.compile('SPAMD/(\d+\.\d+)\s+(\d+)\s+(\S+)')
# Content-length: <size>
content_length_re = re.compile('Content-length:\s+(\d+)')
# Spam: True ; 15 / 5
# Spam: False ; 2 / 5
score_line_re = re.compile('Spam: (\S+) ; (\d+(\.\d+)*) / (\d+(\.\d+)*)')
# X-IronPort-MGA: <large bencoded string>
ironport_mga_re = re.compile('X-IronPort-MGA: (.*)')
# X-IronPort-IPAS-Score: 93.450
ironport_score_re = re.compile('X-IronPort-IPAS-Score: (\d+(\.\d+)*)')


class SpamdClient(object):

    """Class that sends spamd protocol specified commands via a TCP socket
    to a given spamd service.
    """

    def __init__(self, address=('127.0.0.1', 783), connect_timeout=5):
        """Initiate the Client with the address and timeout.

        :Parameters:
            - `address`: Address/port pair that is running spamd server.
            - `connect_timeout`:  Timeout on socket connection.
        """

        self._address = address
        self._connect_timeout = connect_timeout
        if isinstance(self._address, str):
            self._socket_domain = socket.AF_UNIX
        else:
            self._socket_domain = socket.AF_INET

    def _send_command(self, command, data, username=None, case_dict=None):
        """Send message to spamd and wait for response.

        Sends a given command via a raw socket to spamd and tries to recieve
        the verdict for the body.

        :Parameters:
            - `command`: CASE or SpamAssassin compatible command.
            - `data`: Message payload.
            - `username`: Optional field used by spamd to load per-user-config
                          files.
            - `case_dict`: CASE specific RPC data used to configure the CASE
                           engine.

        :Return:
        Returns SpamdResponse object with the proper data fields populated.
        """

        if username:
            username_header = 'User: %s\r\n' % (username,)
        else:
            username_header = ''

        if case_dict:
            case_header = 'X-IronPort-MGA: %s\r\n' % (bencode(case_dict),)
        else:
            case_header = ''

        request = []
        request.append('%s SPAMC/%s\r\n%s%s' %
                    (command, PROTOCOL_VERSION, username_header, case_header))
        request.append('Content-length: %d\r\n\r\n' % (len(data),))
        request.append(data)

        # Setup and tear down the socket for each connection.
        sock = socket.socket(self._socket_domain, socket.SOCK_STREAM)

        sock.settimeout(self._connect_timeout)
        sock.setblocking(1)
        sock.connect(self._address)

        for line in request:
            sock.send(line)

        # Shutdown the sending side of the socket.
        sock.shutdown(socket.SHUT_WR)

        return self._filter(sock)

    def _filter(self, sock):
        """Filter through the received data matching various headers and the
        entire message.

        :Parameters:
            - `sock`: Populated socket object for connecting to spamd instance.

        :Return:
        SpamdResponse object with the proper data fields populated.
        """
        response = SpamdResponse()

        found_eoh_p = False
        for line in lines_from_sock(sock):
            if found_eoh_p:
                # We've already parsed the headers and found the first blank
                # line, the rest of the lines must be the actual message so
                # just record that.

                email_msg_list.append(line)
            else:
                # This should always be the first line read.
                match = response_header_re.match(line)
                if match:
                    response.version = float(match.group(1))
                    response.resp_code = int(match.group(2))
                    response.resp_msg = match.group(3)

                    if response.resp_code != 0:
                        # Something went wrong, return the bad response.
                        return response

                match = score_line_re.match(line)
                if match:
                    response.is_spam = bool(match.group(1) == 'True')

                    response.score = float(match.group(2))
                    response.threshold = float(match.group(4))

                match = content_length_re.match(line)
                if match:
                    response.msg_content_length = match.group(1)

                match = ironport_mga_re.match(line)
                if match:
                    case_data = match.group(1)
                    try:
                        response.decoded_case = bdecode(case_data)
                    except ValueError:
                        raise SpamdClientMalformedRPC('X-IronPort-MGA header '
                                                       'payload malformed: %s'
                                                       % (case_data,))

                match = ironport_score_re.match(line)
                if match:
                    response.ipas_score = match.group(1)

                if not line:
                    # First blank line indicates the end of all headers, the
                    # rest of the response will be all message.
                    found_eoh_p = True
                    email_msg_list = []

        if email_msg_list:
            response.email_msg = ''.join(email_msg_list)

        return response

    def cmd_ping(self):
        """ping()-> test_value, test_data

        :Return:
        Returns true if PONG was found in the data result
        """
        response = self._send_command('PING', '')
        return (response.resp_msg == 'PONG')

    def cmd_check(self, email, username=None, case_dict=None):
        """Returns a SpamdClientDataType object with the decoded RPC verdict.

        :Parameters:
            - `email`: The entire message to scan.
            - `username`: Optional field used by spamd to load per-user-config
                          files.
            - `case_dict`: Optional CASE-specific dictionary of information to
                           encode and send over to case.
                           Example: {'config': {'vof_check': '0',
                                                'spam_check': '1',
                                                'spam_positive': 90,
                                                'did_incoming_relay': 0}}

        :Return:
        SpamdResponse object with the proper data fields populated.
        """
        response = self._send_command('CHECK', email, username=username,
                                      case_dict=case_dict)

        # Check to make sure we got a good response.
        if response.resp_code != 0:
            raise SpamdClientProtocolError(
                'Invalid CHECK response. Response Code: %s Response Message:'
                ' %s' % (response.resp_code, response.resp_msg,))

        return response

    def cmd_process(self, email, username=None):
        """Submits an email to spamd and returns a SpamdClientDataType object.

        :Parameters:
            - `email`: The entire message to process.
            - `username`: Optional field used by spamd to load per-user-config
                          files.

        :Return:
        SpamdResponse object with the proper data fields populated.
        """
        response = self._send_command('PROCESS', email, username=username)

        # Check to make sure we got a good response.
        if response.resp_code != 0:
            raise SpamdClientProtocolError(
                'Invalid PROCESS response. Response Code: %s Response Message:'
                ' %s' % (response.resp_code, response.resp_msg,))

        return response
