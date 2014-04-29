import doctest
import unittest2 as unittest

from shared import auth


class AuthTest(unittest.TestCase):
    """Make this module able to run by "unit2 discover"."""
    def test_auth_init(self):
        doctest.testmod(auth, verbose=False)


if __name__ == '__main__':
    doctest.testmod(auth)
