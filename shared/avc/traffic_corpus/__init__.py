"""Shared module for traffic corpus files parsing related classes and
functions.

The different file objects maintained and parsed using this module are:
traffic corpus object: base class containing common attributes.
pcap file object: contains info regarding a pcap file.
connection object: contains info regarding a tcp connection.
transaction object: contains info regarding http request/response transactions.

:Status: $Id: //prod/main/_is/shared/python/avc/traffic_corpus/__init__.py#4 $
:Author: $Author: kleshche $
"""

import os
import re
import sys
import time

import cjson
import pcap

from shared.avc.traffic_corpus.constants import *


A_KW = 'a'
B_KW = 'b'
P_KW = 'p'

#
# Common classes
#
class PcapFileException(Exception):
    """Pcap file is not valid."""
    pass


class UnsupportedProtocolException(Exception):
    """Protocol is not supported."""
    pass


class FailedStorageException(Exception):
    """File cannot be wriiten to the file system."""
    pass


class DuplicateStorageException(Exception):
    """File already exists in the file system."""
    pass


class UnparsableFileNameException(Exception):
    """File name cannot be parsed."""
    pass


class NoValidTCPPackets(Exception):
    """Pcap file doesn't contain any valid TCP packets"""
    pass


#
# File paths
#
# The file pattern is:
# <some_name>[:a<purported_app>][:p<port>][:b<behavior>].<ext>[.tar]
# Optional items enclosed in square brackets: [].
#

def parse_ingest_file_name(file_name):
    """Parses a file name into its relevant parts for the corpus.

    :Parameters:
        - `file_name`: path to the file

    :Return:
        - `purported_dict['app']`: purported app, string
        - `purported_dict['port']`: purported port, string
        - `purported_dict['behavior']`: purported behavior, string
        - `ext`: provided extension (usually pcap)
        - `tar`: tar extension, either None or 'tar'

    :Exceptions:
        - `UnparsableFileNameException`: file name contains less or more parts
                than required
    """
    file_name_parts = os.path.basename(file_name).split('.')
    tar = None
    ext = None
    if len(file_name_parts) > 3 or len(file_name_parts) < 2:
        raise UnparsableFileNameException('Could not parse: %s', file_name)
    if len(file_name_parts) == 3:
        if file_name_parts[2] == TAR_KW:
            tar = TAR_KW
        else:
            raise UnparsableFileNameException('Could not parse: %s', file_name)

    ext = file_name_parts[1]
    params = file_name_parts[0].split(':')[1:]
    purported_dict = {A_KW: None, B_KW: None, P_KW: None}
    for param in params:
        if param[0] not in purported_dict or \
                purported_dict[param[0]]:
            raise UnparsableFileNameException('Could not parse: %s', file_name)
        purported_dict[param[0]] = param[1:]

    return purported_dict[A_KW], purported_dict[P_KW], \
           purported_dict[B_KW], ext, tar


def get_file_path(base_dir, protocol, timestamp, file_name, suffix):
    """Get the expected file path for an item in the corpus.

    :Parameters:
        - `base_dir`: base directory containing the file, string
        - `protocol`: protocol of transaction in file, string
        - `timestamp`: timstamp when the file was added, string
        - `file_name`: name of the file, string
        - `suffix`: file extension, string
    """
    ts = time.gmtime(timestamp)
    path_parts = [base_dir, protocol]
    path_parts.extend(time.strftime('%Y:%m:%d:%H:%M', ts).split(':'))
    file_name = '%s.%s' % (file_name, suffix)
    path_parts.append(file_name)
    return os.path.join(*path_parts)


class TrafficCorpusObject(object):

    """Class representing a traffic corpus object to store data in traffic
    corpus.
    """

    def __init__(self):
        """Initialization function to declare and initialize class attributes

        :Parameters:
            - None

        :Return:
            - TrafficCorpusObject instance
        """
        self._attributes = {}

    def as_dict(self):
        """Raises an exception if asked to return json encoded dict.

        :Parameters:
            - None

        : Exceptions:
            - `NotImplementedError`: when asked for a json dict for TC object
        """
        raise NotImplementedError

    def as_json(self):
        """Returns a json encoded dictionary

        :Parameters:
            - None

        :Return:
            - json encoded dict as string
        """
        return cjson.encode(self.as_dict())


class PcapObject(TrafficCorpusObject):

    """Class reprsenting a PcapObject to store pcap data in traffic corpus
    """

    def __init__(self, pcap_file_path):
        """Initialization function to declare and initialize clsass attributes

        :Parameters:
            - `pcap_file_path`: path to the pcap file

        :Return:
            - PcapObject instance

        :Exceptions:
            - `PcapFileException`: when pcap file is not valid or does not exist.
        """
        super(PcapObject, self).__init__()
        self.libpcap_obj = pcap.pcapObject()
        self.start_ts = sys.maxint
        self.end_ts = 0
        self.size = 0
        self.number_packets = 0
        self.connections = []

        if not os.path.exists(pcap_file_path):
            raise PcapFileException('pcap file does not exist at %s' %
                                    (pcap_file_path))

        try:
            self.libpcap_obj.open_offline(pcap_file_path)
            # set a blank filter to force libpcap to read the headers of the
            # pcap file to determine if the pcap is indeed valid.
            self.libpcap_obj.setfilter('', 0, 0)
        except:
            raise PcapFileException('%s is not a valid pcap file' % \
                                    (pcap_file_path))
        else:
            self._attributes[WORK_FILE_PATH_KW] = pcap_file_path

    def as_dict(self):
        """Returns information related to pcap

        :Parameters:
            - None

        :Return:
             - `pcap_dict`: dict containing information related to pcap
                     as follows:
                     {'start_ts': <start_time>,
                     'end_ts': <end_time>,
                     'size': <size of the pcap>,
                     'number_packets': <num_packets in the pcap>,
                     'connections': <num_connections in the pcap>}
        """
        connections = [ c.as_dict() for c in self.connections ]
        pcap_dict = {START_TS_KW: self.start_ts,
                     END_TS_KW: self.end_ts,
                     SIZE_KW: self.size,
                     NUM_PACKETS_KW: self.number_packets,
                     CONN_KW: connections}
        return pcap_dict


class Connection(TrafficCorpusObject):

    """Class containing information about a connection
    """

    def __init__(self):
        """Initialization function to declare and initialize class attributes

        :Parameters:
            - None

        :Return:
            - Connection instance
        """
        super(Connection, self).__init__()
        self.connection_data = ''
        self.transactions = []


class TCPConnection(Connection):

    """Class containing information about a connection
    """

    def __init__(self, src_ip, src_port, dest_ip, dest_port):
        """Initialization function to declare and initialize class attributes

        :Parameters:
            - `src_ip`: source ip address
            - `src_port`: source port
            - `dest_ip`: destination ip address
            - `dest_port`: destination port

        :Return:
            - TCPConnection instance
        """
        super(TCPConnection, self).__init__()
        self.src_ip = src_ip
        self.src_port = src_port
        self.dest_ip = dest_ip
        self.dest_port = dest_port
        self.start_ts = sys.maxint
        self.end_ts = 0
        self.connection_data = ''
        self.transactions = []

    def as_dict(self):
        """Returns information related to tcp connection

        :Parameters:
            - None

        :Return:
             - `tcp_connection_dict`: dict containing information related to
                     tcp connection as follows:
                     {'src_ip': <src_ip>,
                      'src_port': <src_port>,
                      'dest_ip': <dest_ip>,
                      'dest_port': <dest_port>,
                      'start_ts': <start_time>,
                      'end_ts': <end_time>}
        """
        tcp_connection_dict = {SRC_IP_KW: self.src_ip,
                               SRC_PORT_KW: self.src_port,
                               DEST_IP_KW: self.dest_ip,
                               DEST_PORT_KW: self.dest_port,
                               START_TS_KW: self.start_ts,
                               END_TS_KW: self.end_ts}
        if self.transactions:
            transactions = []
            # support list or list of request/response tuples
            if isinstance(self.transactions[0], tuple):
                for req, res in self.transactions:
                    req_dict = None
                    res_dict = None
                    if req is not None:
                        req_dict = req.as_dict()
                    if res is not None:
                        res_dict = res.as_dict()
                    transactions.append({REQUEST_KW: req_dict,
                                         RESPONSE_KW: res_dict})
            else:
                transactions = [ t.as_dict() for t in self.transactions if t is not None ]
            tcp_connection_dict[TRANSACT_KW] = transactions
        else:
            tcp_connection_dict[CONN_DATA_KW] = self.connection_data
        return tcp_connection_dict


class Transaction(TrafficCorpusObject):

    """Class containing information related to a transaction
    """

    def __init__(self, transaction_data):
        """Initialization function to declare and initialize class attributes

        :Parameters:
            - `transaction_data`: data inside the transaction

        :Return:
            - Transactionn instance
        """
        super(Transaction, self).__init__()
        self.size = len(transaction_data)
        self.transaction_data = transaction_data


class TCPTransaction(Transaction):

    """Class containing information related to a tcp transaction
    """

    def __init__(self, src_ip, src_port, dest_ip, dest_port, transaction_data):
        """Initialization function to declare and initialize class attributes

        :Parameters:
            - `src_ip`: source ip address
            - `src_port`: source port
            - `dest_ip`: destination ip address
            - `dest_port`: destination port
            - `transaction_data`: data in transaction

        :Return:
            - TCPTransaction instance
        """
        super(TCPTransaction, self).__init__(transaction_data)
        self.src_ip = src_ip
        self.src_port = src_port
        self.dest_ip = dest_ip
        self.dest_port = dest_port

    def as_dict(self):
        """Returns information related to tcp transaction

        :Parameters:
            - None

        :Return:
             - `transaction_dict`: dict containing information related to
                     tcp transaction as follows:
                     {'src_ip': <src_ip>,
                      'src_port': <src_port>,
                      'dest_ip': <dest_ip>,
                      'dest_port': <dest_port>,
                      'size': <transaction_size>,
                      'transaction_data': <data_in_transaction>}
        """
        transaction_dict = {SRC_IP_KW: self.src_ip,
                            SRC_PORT_KW: self.src_port,
                            DEST_IP_KW: self.dest_ip,
                            DEST_PORT_KW: self.dest_port,
                            SIZE_KW: self.size,
                            TRANSACT_DATA_KW: self.transaction_data}
        return transaction_dict


class HTTPRequest(TrafficCorpusObject):

    """Class containing information related to a http request transaction
    """

    def __init__(self, http_version, http_method, path, headers, raw_headers,
                 raw_content, rendered_content):
        """Initialization function to declare and initialize class attributes

        :Parameters:
            - `http_version`: version of the http protocol
            - `http_method`: http method GET/ POST used
            - `path`: path to the transaction file
            - `headers`: headers in the transaction
            - `raw_headers`: unparsed headers
            - `raw_content`: unparsed content
            - `rendered_content`: rendered content

        :Return:
            - HTTPRequest instance
        """
        super(HTTPRequest, self).__init__()
        self.http_version = http_version
        self.http_method = http_method
        self.path = path
        self.headers = headers
        self.raw_headers = raw_headers
        self.raw_content = raw_content
        self.rendered_content = rendered_content

    def as_dict(self):
        """Returns information related to http request

        :Parameters:
            - None

        :Return:
             - `http_transaction_dict`: dict containing information related to
                     http request transaction as follows:
                     {'http_version': <http_protocol_version>,
                      'http_method': <http_method>,
                      'path': <path_to_request_file>,
                      'headers': <headers_data>,
                      'raw_content': <raw_data>,
                      'rendered_content': <rendered_content>}

        """
        http_transaction_dict = {HTTP_VER_KW: self.http_version,
                                 HTTP_METH_KW: self.http_method,
                                 PATH_KW: self.path,
                                 HDRS_KW: self.headers,
                                 RAW_CONTENT_KW: self.raw_content,
                                 RNDRD_CONTENT_KW: self.rendered_content}
        return http_transaction_dict


class HTTPResponse(TrafficCorpusObject):

    """Class containing information related to a http request transaction
    """

    def __init__(self, http_version, http_code, reason, headers, raw_headers,
                 raw_content, rendered_content):
        """Initialization function to declare and initialize class attributes

        :Parameters:
            - `http_version`: version of the http protocol
            - `http_code`: http return code
            - `reason`: response reason
            - `headers`: headers in the transaction
            - `raw_headers`: unparsed headers
            - `raw_content`: unparsed content
            - `rendered_content`: rendered content

        :Return:
            - HTTPResponse instance
        """
        super(HTTPResponse, self).__init__()
        self.http_version = http_version
        self.http_code = http_code
        self.reason = reason
        self.headers = headers
        self.raw_headers = raw_headers
        self.raw_content = raw_content
        self.rendered_content = rendered_content
        self._attributes = {}

    def as_dict(self):
        """Returns information related to http request

        :Parameters:
            - None

        :Return:
             - `http_transaction_dict`: dict containing information related to
                     http request transaction as follows:
                     {'http_version': <http_protocol_version>,
                      'http_code': <response_type_code>,
                      'reason': <response_reason>,
                      'headers': <headers_data>,
                      'raw_content': <raw_data>,
                      'rendered_content': <rendered_content>}

        """
        http_transaction_dict = {HTTP_VER_KW: self.http_version,
                                 HTTP_CODE_KW: self.http_code,
                                 REASON_KW: self.reason,
                                 HDRS_KW: self.headers,
                                 RAW_CONTENT_KW: self.raw_content,
                                 RNDRD_CONTENT_KW: self.rendered_content}
        return http_transaction_dict

