"""Mocks for shared.dbqueue modules.

:Status: $Id: //prod/main/_is/shared/python/dbqueue/test/mockobjects.py#2 $
:Authors: vburenin
"""

import MySQLdb.cursors
import contextlib
from shared.db.dbcp import pool

class ConnectionPoolMock:
    def __init__(self, mock_control):
        if (not hasattr(pool.ConnectionPool, 'transaction')):
            raise Exception('shared.db.pool.ConnectionPool ' \
                            'interface was changed')
        self.cursor_mock = mock_control.mock_class(MySQLdb.cursors.SSCursor,
                                                   display_name='cursor')

    @contextlib.contextmanager
    def transaction(self, timeout=None, cursor_class=None):
        yield self.cursor_mock
