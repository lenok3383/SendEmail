"""
:Status: $Id: //prod/main/_is/shared/python/avc/traffic_corpus/demo.py#1 $
:Author: $Author: usarfraz $

The demo module is used for testing the API in shared.avc.traffic_corpus.
"""

import logging
import optparse
import os
import pprint
import sys

from shared.avc import traffic_corpus
from shared.avc.traffic_corpus import connection_extractor, http_extractor


def main():
    """Main entry point"""

    usage_desc = """Usage: python demo.py <pcap_fpath_1> <pcap_fpath_2> ...

Program to demonstrate the usage of shared.avc.traffic_corpus module

The demo program demonstates how to get a list of tcp connections in a pcap
session object. Then it gets a list of transaction(request, response) pairs
for each of the tcp connection objects.
For example:
  python demo.py /path-to-pcap-dir/file-name1.pcap ...:

It means that you have to input paths for one or more pcap files whose
connection and transaction information is to be retrieved.

Note! At least one argument should be passed in as a path to pcap file
for the script to process the pcap. If wrong path is specified, an exception
is raised:
For example:
python demo.py /invalid/path/to/pcap_file

Traceback (most recent call last):
  File "demo.py", line 59, in <module>
    main()
  File "demo.py", line 48, in main
    pcap = traffic_corpus.PcapObject(path)
  File "/home/usarfraz/tc/src/python/shared/avc/traffic_corpus/__init__.py", line 179, in __init__
    raise PcapFileException('%s is not valid pcap file' % (pcap_file_path))
shared.avc.traffic_corpus.PcapFileException: test is not valid pcap file
"""

    # Parse command-line options.
    option_parser = optparse.OptionParser(usage=usage_desc)
    (options, args) = option_parser.parse_args()

    if len(args) == 0:
        option_parser.print_usage()

    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    for path in sys.argv[1:]:
        pcap = traffic_corpus.PcapObject(path)
        tcp_conns = connection_extractor.get_tcp_connections(pcap)
        pcap.connections.extend(tcp_conns)

        for conn in tcp_conns:
            pprint.pprint(conn.as_dict())
            request_responses = http_extractor.get_http_transactions(conn)
            conn.transactions = request_responses
        pprint.pprint(pcap.as_dict())

if __name__ == '__main__':
    main()
