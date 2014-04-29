import doctest
import unittest2 as unittest

from shared.process import daemon


class DaemonTest(unittest.TestCase):
    """Make this module able to run by "unit2 discover"."""
    def test_daemon(self):
        doctest.testmod(daemon, verbose=False)


if __name__ == '__main__':
    doctest.testmod(daemon)
