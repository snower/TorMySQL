# -*- coding: utf-8 -*-
# 16/3/25
# create by: snower

from tornado import gen
from .pool import ConnectionPool as BaseConnectionPool
from . import log

class TransactionClosedError(Exception):
    pass

class Transaction(object):
    def __init__(self, pool, connection):
        self._pool = pool
        self._connection = connection

    def _ensure_conn(self):
        if self._connection is None:
            raise TransactionClosedError("Transaction is closed already.")

    @gen.coroutine
    def execute(self, query, params=None, cursor_cls=None):
        self._ensure_conn()
        cursor = self._connection.cursor(cursor_cls)
        try:
            yield cursor.execute(query, params)
        finally:
            yield cursor.close()
        raise gen.Return(cursor)

    @gen.coroutine
    def executemany(self, query, params=None, cursor_cls=None):
        self._ensure_conn()
        cursor = self._connection.cursor(cursor_cls)
        try:
            yield cursor.executemany(query, params)
        finally:
            yield cursor.close()
        raise gen.Return(cursor)

    @gen.coroutine
    def commit(self):
        self._ensure_conn()
        yield self._connection.commit()
        self._connection.close()
        self._connection = None

    @gen.coroutine
    def rollback(self):
        self._ensure_conn()
        yield self._connection.rollback()
        self._connection.close()
        self._connection = None

    def __del__(self):
        if self._connection:
            log.get_log().warning("Transaction has not committed or rollbacked %s.", self._connection)
            self._connection.do_close()
            self._connection = None

class ConnectionPool(BaseConnectionPool):
    def __init__(self, *args, **kwargs):
        super(ConnectionPool, self).__init__(*args, **kwargs)

    @gen.coroutine
    def execute(self, query, params=None, cursor_cls=None):
        with (yield self.Connection()) as connection:
            cursor = connection.cursor(cursor_cls)
            try:
                yield cursor.execute(query, params)
            finally:
                yield cursor.close()
        raise gen.Return(cursor)

    @gen.coroutine
    def executemany(self, query, params=None, cursor_cls=None):
        with (yield self.Connection()) as connection:
            cursor = connection.cursor(cursor_cls)
            try:
                yield cursor.executemany(query, params)
            finally:
                yield cursor.close()
        raise gen.Return(cursor)

    @gen.coroutine
    def begin(self):
        connection = yield self.Connection()
        try:
            yield connection.begin()
        except:
            connection.close()
            raise
        transaction = Transaction(self, connection)
        raise gen.Return(transaction)