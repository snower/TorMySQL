# -*- coding: utf-8 -*-
# 16/3/25
# create by: snower

import sys
from . import platform
try:
    from tornado.util import raise_exc_info
except ImportError:
    def raise_exc_info(exc_info):
        try:
            raise exc_info[1].with_traceback(exc_info[2])
        finally:
            exc_info = None

from .pool import ConnectionPool as BaseConnectionPool
from . import log

try:
    from tornado.gen import Return
except ImportError:
    pass
from .util import py3


class TransactionClosedError(Exception):
    pass


class Transaction(object):
    def __init__(self, pool, connection):
        self._pool = pool
        self._connection = connection

    def _ensure_conn(self):
        if self._connection is None:
            raise TransactionClosedError("Transaction is closed already.")

    if py3:
        exec("""
async def execute(self, query, params=None, cursor_cls=None):
    self._ensure_conn()
    async with self._connection.cursor(cursor_cls) as cursor:
        await cursor.execute(query, params)
        return cursor

async def executemany(self, query, params=None, cursor_cls=None):
    self._ensure_conn()
    async with self._connection.cursor(cursor_cls) as cursor:
        await cursor.executemany(query, params)
        return cursor

async def commit(self):
    self._ensure_conn()
    try:
        await self._connection.commit()
    except:
        exc_info = sys.exc_info()
        self._connection.close(True)
        raise_exc_info(exc_info)
    else:
        self._connection.close()
    finally:
        self._connection = None

async def rollback(self):
    self._ensure_conn()
    try:
        await self._connection.rollback()
    except:
        exc_info = sys.exc_info()
        self._connection.close(True)
        raise_exc_info(exc_info)
    else:
        self._connection.close()
    finally:
        self._connection = None

async def __aenter__(self):
    return self

async def __aexit__(self, exc_type, exc_val, exc_tb):
    if exc_type:
        await self.rollback()
    else:
        await self.commit()
        """)
    else:
        @platform.coroutine
        def execute(self, query, params=None, cursor_cls=None):
            self._ensure_conn()
            cursor = self._connection.cursor(cursor_cls)
            try:
                yield cursor.execute(query, params)
            finally:
                yield cursor.close()
            raise Return(cursor)

        @platform.coroutine
        def executemany(self, query, params=None, cursor_cls=None):
            self._ensure_conn()
            cursor = self._connection.cursor(cursor_cls)
            try:
                yield cursor.executemany(query, params)
            finally:
                yield cursor.close()
            raise Return(cursor)

        @platform.coroutine
        def commit(self):
            self._ensure_conn()
            try:
                yield self._connection.commit()
            except:
                exc_info = sys.exc_info()
                self._connection.close(True)
                raise_exc_info(exc_info)
            else:
                self._connection.close()
            finally:
                self._connection = None

        @platform.coroutine
        def rollback(self):
            self._ensure_conn()
            try:
                yield self._connection.rollback()
            except:
                exc_info = sys.exc_info()
                self._connection.close(True)
                raise_exc_info(exc_info)
            else:
                self._connection.close()
            finally:
                self._connection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            log.get_log().warning("Transaction has not committed or rollbacked %s.", self._connection)

    def __del__(self):
        if self._connection:
            log.get_log().warning("Transaction has not committed or rollbacked %s.", self._connection)
            self._connection.do_close()
            self._connection = None


class ConnectionPool(BaseConnectionPool):
    def __init__(self, *args, **kwargs):
        super(ConnectionPool, self).__init__(*args, **kwargs)

    if py3:
        exec("""
async def execute(self, query, params=None, cursor_cls=None):
    async with await self.Connection() as connection:
        async with connection.cursor(cursor_cls) as cursor:
            await cursor.execute(query, params)
            return cursor

async def executemany(self, query, params=None, cursor_cls=None):
    async with await self.Connection() as connection:
        async with connection.cursor(cursor_cls) as cursor:
            await cursor.executemany(query, params)
            return cursor

async def begin(self):
    connection = await self.Connection()
    try:
        await connection.begin()
    except:
        exc_info = sys.exc_info()
        connection.close()
        raise_exc_info(exc_info)
    transaction = Transaction(self, connection)
    return transaction
        """)
    else:
        @platform.coroutine
        def execute(self, query, params=None, cursor_cls=None):
            with (yield self.Connection()) as connection:
                cursor = connection.cursor(cursor_cls)
                try:
                    yield cursor.execute(query, params)
                    if not connection._connection.autocommit_mode:
                        yield connection.commit()
                except:
                    exc_info = sys.exc_info()
                    if not connection._connection.autocommit_mode:
                        yield connection.rollback()
                    raise_exc_info(exc_info)
                finally:
                    yield cursor.close()
            raise Return(cursor)

        @platform.coroutine
        def executemany(self, query, params=None, cursor_cls=None):
            with (yield self.Connection()) as connection:
                cursor = connection.cursor(cursor_cls)
                try:
                    yield cursor.executemany(query, params)
                    if not connection._connection.autocommit_mode:
                        yield connection.commit()
                except:
                    exc_info = sys.exc_info()
                    if not connection._connection.autocommit_mode:
                        yield connection.rollback()
                    raise_exc_info(exc_info)
                finally:
                    yield cursor.close()
            raise Return(cursor)

        @platform.coroutine
        def begin(self):
            connection = yield self.Connection()
            try:
                yield connection.begin()
            except:
                exc_info = sys.exc_info()
                connection.close()
                raise_exc_info(exc_info)
            transaction = Transaction(self, connection)
            raise Return(transaction)