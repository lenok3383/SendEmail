"""Mocks for shared.db modules unit testing.

:Status: $Id: //prod/main/_is/shared/python/db/test/mocks.py#10 $
:Authors: ohmelevs
"""

import contextlib
import time

from shared.db import errors
from shared.db.dbcp import mysql
from shared.db.dbcp.connection_factory import BaseConnectionFactory, BaseCursor


def connect(db, host, user, passwd, compress, connect_timeout, cursorclass):
    return ConnectionMock()


def _get_factory(config, name, read_write, isolation_level=None,
                 session_options=None):
    return FactoryMock(config, name, read_write)


class ConnectionMock(object):

    def __init__(self, *args, **kwargs):
        #test variables
        self._is_valid = True
        self._sleep_time = 0

    def ping(self):
        if not self._is_valid:
            raise mysql.MySQLdb.OperationalError
        time.sleep(self._sleep_time)

    def close(self):
        pass

    def cursor(self, cursor_class=None):
        return MySQLdbCursorMock()


class MySQLdbCursorMock(BaseCursor):

    def __init__(self, *args, **kwargs):
        self.executed_queries = list()
        super(MySQLdbCursorMock, self).__init__(*args, **kwargs)

    def _execute_impl(self, query, args=None):
        if args:
            query = query % args
        else:
            query = query
        self.executed_queries.append(query)
        return query

    def _executemany_impl(self, query, args):
        res = list()
        for i in args:
            res.append(query % i)
            self.executed_queries.append(query % i)
        return res

    def close(self):
        pass


class FactoryMock(BaseConnectionFactory):

    def __init__(self, config, pool_name, read_write=False):
        BaseConnectionFactory.__init__(self, config, pool_name, read_write)

        if not config:
            return

        # Simulate exceptions for missing parameters.
        if read_write:
            key = pool_name + '.rw.host'
        else:
            key = pool_name + '.ro.host'
        if key not in config:
            raise errors.DBConfigurationError(
                'missing val in config %s' % (key,))

    def is_valid(self, conn, timeout=None):
        return True

    def get_conn(self):
        return ConnectionMock()

    @contextlib.contextmanager
    def error_translation(self):
        try:
            yield
        except Exception:
            raise

class ConnectionPoolMock(object):

    def __init__(self, size, factory, timeout, idle):
        self.conn_factory = factory

    def shutdown(self):
        pass

    def connection(self, timeout=None):
        return CursorStub(MySQLdbCursorMock())

    def transaction(self, timeout=None, cursor_class=None):
        return CursorStub(MySQLdbCursorMock())

    def close_idle_connections(self):
        pass


class CursorStub(object):

    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self._cursor

    def __exit__(self, exc_type, value, traceback):
        pass


class ContextStub(object):

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass


class LogMock:

    def __init__(self):
        self.logged = list()

    def error(self, query, *args, **kwargs):
        self.logged.append(query % args)

    def debug(self, query, *args, **kwargs):
        self.logged.append(query % args)

    def warning(self, query, *args, **kwargs):
        self.logged.append(query % args)

    def exception(self, query, *args, **kwargs):
        self.logged.append(query % args)

    def is_logged(self, arg):
        for q in self.logged:
            if q.find(arg) != -1:
                return True
        return False


