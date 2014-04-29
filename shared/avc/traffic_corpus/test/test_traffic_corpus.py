import os
import unittest

from shared.avc import traffic_corpus
from shared.avc.traffic_corpus import connection_extractor, http_extractor
from shared.avc.traffic_corpus.constants import *

SAMPLE_DATA_DIR = 'sample_data'


class TestCorpus(unittest.TestCase):

    """Class to test connection_extractor module
    """

    def setUp(self):
        """Sets up the environment"""
        self.sample_dir = SAMPLE_DATA_DIR

    def tearDown(self):
        """Clears up the environment."""
        pass

    def test_invalid_pcap(self):
        """Test that invalid pcap path raises exception."""
        path = os.path.join(self.sample_dir, 'invalid.pcap')
        self.assertRaises(traffic_corpus.PcapFileException,
                         traffic_corpus.PcapObject, path)

    def test_nonexistent_pcap(self):
        """Test that exception is raised if pcap file does not exist."""
        self.assertRaises(traffic_corpus.PcapFileException,
                         traffic_corpus.PcapObject, '')

    def test_file_name_parts(self):
        """Test that file name is parsed into relevant parts."""
        path = os.path.join(self.sample_dir,
                            'aim:port|80:app|im:behavior|generic.pcap.tar')
        app, port, behavior, ext, tar = \
                traffic_corpus.parse_ingest_file_name(path)
        self.assertEquals(app, 'im')
        self.assertEquals(port, '80')
        self.assertEquals(behavior, 'generic')
        self.assertEquals(ext, 'pcap')
        self.assertEquals(tar, 'tar')

    def test_bad_file_name(self):
        """Test that exception is raised when file name is invalid."""
        path = os.path.join(self.sample_dir,
                            'aim:80:im:generic.pcap:tar')
        self.assertRaises(traffic_corpus.UnparsableFileNameException,
                          traffic_corpus.parse_ingest_file_name, path)

    def test_get_file_path(self):
        """Test that file path is return for an item in corpus."""
        path = traffic_corpus.get_file_path(
                self.sample_dir, 'http', 123456789, '1436', 'request')
        print path

    def test_get_tcp_connections(self):
        """Test that a list of connection objects is fetched for pcap object"""
        path = os.path.join(self.sample_dir, 'aim.pcap')
        pcap_obj = traffic_corpus.PcapObject(path)
        tcp_conns = connection_extractor.get_tcp_connections(pcap_obj)

        conn_dict = tcp_conns[0].as_dict()
        self.assertEquals(conn_dict[SRC_IP_KW], '64.12.236.15')
        self.assertEquals(conn_dict[DEST_IP_KW], '10.21.127.164')
        self.assertEquals(conn_dict[SRC_PORT_KW], 80)
        self.assertEquals(conn_dict[DEST_PORT_KW], 16125)

    def test_get_udp_data(self):
        """Test that decoding udp data raises exception."""
        self.assertRaises(traffic_corpus.UnsupportedProtocolException,
                          connection_extractor.decode_udp, '')

    def test_get_http_transactions(self):
        """Test that a list of request/response pairs is fetched for connection object"""
        path = os.path.join(self.sample_dir, 'aim.pcap')
        pcap_obj = traffic_corpus.PcapObject(path)
        tcp_conns = connection_extractor.get_tcp_connections(pcap_obj)
        transacts = http_extractor.get_http_transactions(tcp_conns[0])
        print transacts[0][0].as_dict()
        print '\n\n'
        print transacts[0][1].as_dict()


if __name__ == '__main__':
    # Run unit tests.
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCorpus)
    unittest.TextTestRunner(verbosity=2).run(suite)

