"""
:Status: $Id: //prod/main/_is/shared/python/avc/traffic_corpus/connection_extractor.py#2 $
:Author: $Author: kleshche $

The connection extractor module extracts Connection Objects from Pcap files.
Initially only TCP Connection extraction is provided, though additional
functions to handle other protocols should be added.
"""

import logging
import socket
import struct
import sys

import pcap

from shared.avc import traffic_corpus
from shared.avc.traffic_corpus import UnsupportedProtocolException
from shared.avc.traffic_corpus.constants import *

_LOG = logging.getLogger(__file__)


def decode_ip(ip_data):
    """Parse ip data according to libpcap format

    :Parameters:
        - `ip_data`: libpcap ip data payload

    :Return:
        - `decoded_ip`: decoded ip address
        - `payload`: decoded_payload
    """
    hdr, tos, size, packet_id, flags, ttl, prot, chksum, src_addr, dst_addr = \
        struct.unpack('BBHHHBBHII', ip_data[:20])
    decoded_ip = {}
    decoded_ip[HDR_LEN_KW] = hdr & 0x0f
    decoded_ip[TOT_LEN_KW] = socket.ntohs(size)
    decoded_ip[PROTO_KW] = prot
    decoded_ip[SRC_IP_KW] = pcap.ntoa(src_addr)
    decoded_ip[DEST_IP_KW] = pcap.ntoa(dst_addr)
    payload = ip_data[4 * decoded_ip[HDR_LEN_KW]:]
    return decoded_ip, payload


def decode_tcp(tcp_data):
    """Parse tcp data according to libpcap format

    :Parameters:
        - `tcp_data`: libpcap tcp data payload

    :Return:
        - `decoded_tcp`: decoded tcp header data
        - `payload`: decoded_payload
    """
    s_port, d_port, seq, ack, off_flags, win, checksum, urp = \
        struct.unpack('HHIIHHHH', tcp_data[:20])
    decoded_tcp = {}
    decoded_tcp[SRC_PORT_KW] = socket.ntohs(s_port)
    decoded_tcp[DEST_PORT_KW] = socket.ntohs(d_port)
    decoded_tcp[SEQ_NUM_KW] = socket.ntohl(seq)
    decoded_tcp[DATA_OFFSET_KW] = socket.ntohs(off_flags) >> 12
    payload = tcp_data[decoded_tcp[DATA_OFFSET_KW] * 4:]
    return decoded_tcp, payload


def decode_udp(udp_data):
    """Raises an exception if asked to parse udp data

    :Parameters:
        - `udp_data`: libpcap udp data payload

    :Exceptions:
        - `UnsupportedProtocolException`: when asked for decoded udp data
    """
    raise UnsupportedProtocolException(
        'UDP data is not yet supported.')


def _close_open_connections(open_connections, closed_connections):
    """Take dictionaries representing open and closed connections, and append
    all open connections to the closed connections, then order the packet data,
    filter out empty packets (no payload), and dedupe in case of retransmission.

    :Parameters:
        - `open_connections`: dictionary of packets organized by connection
        - `closed_connections`: dictionary of packets organized by connection

    :Return:
        - `tcp_connections`: a list of TCPConnection objects
    """
    connections_timestamp = {}

    for conn_tup in open_connections:
        r_conn_tup = (conn_tup[2], conn_tup[3], conn_tup[0], conn_tup[1])
        if r_conn_tup in closed_connections:
            closed_transactions = closed_connections[r_conn_tup]
        elif conn_tup in closed_connections:
            closed_transactions = closed_connections[conn_tup]
        else:
            closed_connections[conn_tup] = []
            closed_transactions = closed_connections[conn_tup]
        closed_transactions.append(open_connections[conn_tup])
    del open_connections

    if closed_connections:
        for conn_tup, transaction_dicts in closed_connections.iteritems():
            indexes_for_removal = []
            index = -1
            start_ts = sys.maxint
            end_ts = 0
            for transaction_dict in transaction_dicts:
                # Dedupe by inserting in a dictionary.
                payload_dict = {}
                for sequence_tup, timestamp, payload in transaction_dict[TRANSACT_KW]:
                    start_ts = min(start_ts, timestamp)
                    end_ts = max(end_ts, timestamp)
                    payload_dict[sequence_tup] = payload
                payload_list = payload_dict.items()
                # Sort based on sequence tuple.
                payload_list.sort(key=lambda x: x[0])
                concated_payload = [ x[1] for x in payload_list ]
                # In case the payload is empty, remove this transaction.
                index += 1
                if not concated_payload:
                    indexes_for_removal.append(index)
                    continue
                # Replace the list of payload tuples with combined payload in bytes
                transaction_dict[PAYLD_KW] = ''.join(concated_payload)
            for i in reversed(indexes_for_removal):
                del transaction_dicts[i]
            connections_timestamp[conn_tup] = (start_ts, end_ts)

    tcp_connections = []
    for conn_tup, transaction_dicts in closed_connections.iteritems():
        src_ip, src_port, dest_ip, dest_port = conn_tup
        if transaction_dicts:
            tcp_conn = traffic_corpus.TCPConnection(src_ip, src_port,
                                                dest_ip, dest_port)
            tcp_conn.start_ts = connections_timestamp[conn_tup][0]
            tcp_conn.end_ts = connections_timestamp[conn_tup][1]
            for transaction_dict in transaction_dicts:
                tcp_transact = traffic_corpus.TCPTransaction(
                    transaction_dict[CONN_DICT_KW][SRC_IP_KW],
                    transaction_dict[CONN_DICT_KW][SRC_PORT_KW],
                    transaction_dict[CONN_DICT_KW][DEST_IP_KW],
                    transaction_dict[CONN_DICT_KW][DEST_PORT_KW],
                    transaction_dict[PAYLD_KW])
                tcp_conn.transactions.append(tcp_transact)
            tcp_connections.append(tcp_conn)
    return tcp_connections


def _process_tcp(tcp_data, supported_ports):
    """Take tcp_data (a list of tuples/packets of the form: ip_dict, ip_payload,
    timestamp), and return a list of TCPConnection objects.

    :Parameters:
        - `tcp_data`: a list of tuples/packets of the form: ip_dict, ip_payload,
                timestamp
        - `supported_ports`: a list (may be empty) of ports which we care about.
                Connections to different ports will be dropped, unless empty list

    :Return:
        - `tcp_connections`: a list of TCPConnection objects
    """
    open_connections = {}
    closed_connections = {}

    for ip_dict, ip_payload, timestamp in tcp_data:
        conn_dict, conn_payload = decode_tcp(ip_payload)
        conn_dict[SRC_IP_KW] = ip_dict[SRC_IP_KW]
        conn_dict[DEST_IP_KW] = ip_dict[DEST_IP_KW]

        if not conn_payload:
            continue

        # Create the keys for our open connections dictionary.
        conn_tup = (ip_dict[SRC_IP_KW],
                    conn_dict[SRC_PORT_KW],
                    ip_dict[DEST_IP_KW],
                    conn_dict[DEST_PORT_KW])
        r_conn_tup = (ip_dict[DEST_IP_KW],
                      conn_dict[DEST_PORT_KW],
                      ip_dict[SRC_IP_KW],
                      conn_dict[SRC_PORT_KW])
        packet_tuple = (conn_dict[SEQ_NUM_KW], timestamp, conn_payload)

        # If we have an existing open connection, append to it.
        if conn_tup in open_connections:
            open_connections[conn_tup][TRANSACT_KW].append(packet_tuple)
            continue
        # If there is an open connection in the opposite direction, close it.
        elif r_conn_tup in open_connections:
            if r_conn_tup in closed_connections:
                closed_transactions = closed_connections[r_conn_tup]
            elif conn_tup in closed_connections:
                closed_transactions = closed_connections[conn_tup]
            else:
                closed_connections[conn_tup] = []
                closed_transactions = closed_connections[conn_tup]
            closed_transactions.append(open_connections[r_conn_tup])
            del open_connections[r_conn_tup]

        # Start a new connection.
        if conn_dict[DEST_PORT_KW] in supported_ports \
            or conn_dict[SRC_PORT_KW] in supported_ports:
            open_connections[conn_tup] = {CONN_DICT_KW: conn_dict,
                                          TRANSACT_KW: [packet_tuple]}
        elif not supported_ports:
            open_connections[conn_tup] = {CONN_DICT_KW: conn_dict,
                                          TRANSACT_KW: [packet_tuple]}
        else:
            _LOG.debug(
                'Connection does not use a supported port [%s:%s]' % (
                conn_dict[DEST_PORT_KW], conn_dict[SRC_PORT_KW]))
    tcp_connections = \
        _close_open_connections(open_connections, closed_connections)
    return tcp_connections


def _process_pcap(pcap_data):
    """Take pcap_data which is a list of (packet_len, data, timestamp) tuples
    which represent individual packets, and return a dictionary of the
    ip payload organized by protocol (tcp, udp, etc.)

    :Parameters:
        `pcap_data`: a list of (packet_len, data, timestamp) tuples

    :Return:
        - `ip_protocol_data`: dictionary of ip payload organized by protocol
                {socket.IPPROTO_TCP: [(ip_dict, ip_payload, timestamp), ...],
                 socket.IPPROTO_UDP: [(ip_dict, ip_payload, timestamp), ...],
                 ...}
    """
    ip_protocol_data = {}
    invalid_packets = 0
    for packet_len, packet_data, timestamp in pcap_data:
        if packet_data[12:14] == '\x08\x00': #IP
            ip_data = packet_data[14:]
            ip_dict, ip_payload = decode_ip(ip_data)

            if not ip_dict[PROTO_KW] in ip_protocol_data:
                ip_protocol_data[ip_dict[PROTO_KW]] = []
            ip_protocol_data[ip_dict[PROTO_KW]].append(
                (ip_dict, ip_payload, timestamp))
        else:
            invalid_packets += 1
    if invalid_packets:
        _LOG.debug('Found %d invalid IP packets in the pcap', invalid_packets)
    return ip_protocol_data


def get_tcp_connections(pcap_object, supported_ports=None, libpcap_filter=None):
    """Take a PcapObject, a list of supported ports (optional), and a libpcap
    filter (optional) and return a list of TCPConnection objects.

    :Parameters:
        - `pcap_object`: PcapObject containing a number of connections.
        - `supported_ports`: only return connections whose source or destination
                is one of the supported ports.
        - `libpcap_filter`: only return connections whose packets pass through
                the libpcap filter.

    :Return:
        - `tcp_connections`: a list of TCPConnection objects"""
    if supported_ports is None:
        supported_ports = []
    pcap_object.libpcap_obj.setfilter(libpcap_filter, 0, 0)
    num_packets = 0
    pcap_data = []
    tcp_connections = []
    while True:
        tup = pcap_object.libpcap_obj.next()
        if tup is None:
            break
        num_packets += 1
        packet_len, data, timestamp = tup
        pcap_object.start_ts = min(pcap_object.start_ts, int(timestamp))
        pcap_object.end_ts = max(pcap_object.end_ts, int(timestamp))
        pcap_object.size += packet_len
        pcap_data.append((packet_len, data, timestamp))
    pcap_object.number_packets = num_packets
    ip_protocol_data = _process_pcap(pcap_data)
    if socket.IPPROTO_TCP in ip_protocol_data:
        tcp_connections = _process_tcp(ip_protocol_data[socket.IPPROTO_TCP],
                                   supported_ports)
    else:
        raise traffic_corpus.NoValidTCPPackets(
            'No valid TCP packets found in pcap.')
    _LOG.debug('pcap file contained %s packets' % (num_packets))
    return tcp_connections
