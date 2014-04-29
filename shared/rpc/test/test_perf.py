"""
Test performance for FastRPC client/server

:Authors: vkuznets
"""

import logging
import time

from shared.rpc.blocking_server import FastRPCServer
from shared.rpc.client import FastRPCClient
from shared.rpc.test import TestRoot

logging.basicConfig(level=logging.INFO)


class TestFastRPCPerformance:

    """Unittest to test real client/server pair"""

    def __init__(self):
        self.log = logging.getLogger('RPC Performance Test')
        self.addr = ('0.0.0.0', 13999)
        self.server = FastRPCServer(TestRoot(), self.addr)
        self.server.start()
        time.sleep(1)
        self.client = FastRPCClient(self.addr, True)
        self.remote_root = self.client.get_proxy()

    def shutdown(self):
        """
        Stop server/client
        """
        self.client.close()
        self.server.kill()
        self.server.join()

    def run(self):
        """
        Test remote method calls
        """
        n_calls = 10000
        t_start = time.time()
        for _ in xrange(n_calls):
            self.remote_root.mul(3, 5)
        t_elaps = time.time() - t_start
        self.log.info('Done %d calls in %.3f seconds. Perf: %.3f qps', n_calls,
                                                                       t_elaps,
                                                             n_calls / t_elaps)


if __name__ == "__main__":
    perf_test = TestFastRPCPerformance()
    try:
        perf_test.run()
    finally:
        perf_test.shutdown()
