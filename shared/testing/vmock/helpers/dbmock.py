"""Database transaction mocking.

:Status: $Id: //prod/main/_is/shared/python/testing/vmock/helpers/dbmock.py#1 $
:Authors: vburenin, gmazzola

Usage
-----

class TestFoo(unittest2.TestCase):
    def setUp(self):
        self.mc = mockcontrol.MockControl()
        self.pool_mock = dbmock.DBPoolManagerMock(self.mc)
        self.mc.stub_method(dbcp, 'DBPoolManager')().returns(self.pool_mock)
        self.cursor_mock = self.pool_mock.cursor_mock

    def test_foo(self):
        self.cursor_mock.execute('SELECT foo, bar FROM foo_table')
        self.cursor_mock.fetchone().returns((1, 2))

        self.mc.replay()
        obj.foo()
        self.mc.verify()
"""

import MySQLdb.cursors
import contextlib

from shared.db.dbcp import pool


class DBPoolManagerMock(object):
    """Mock helper for Connection pool manager.

    Can be used if class operates with pool manager instead decorated functions.
    """

    def __init__(self, mock_control):
        """Constructor.

        :param mock_control: MockControl instance.
        """
        if (not hasattr(pool.ConnectionPool, 'transaction')):
            raise Exception('shared.db.pool.ConnectionPool ' \
                            'interface was changed')
        self.__cursor_mock = mock_control.mock_class(MySQLdb.cursors.SSCursor,
                                                     display_name='cursor')

    def get_ro_pool(self, *args, **kwargs):
        return self

    def get_rw_pool(self, *args, **kwargs):
        return self

    @property
    def cursor_mock(self):
        """Cursor mock object."""
        return self.__cursor_mock

    @contextlib.contextmanager
    def transaction(self, timeout=None, cursor_class=None):
        yield self.cursor_mock
