# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from . import platform
from pymysql.cursors import (
    Cursor as OriginCursor, DictCursor as OriginDictCursor,
    SSCursor as OriginSSCursor, SSDictCursor as OriginSSDictCursor)
from .util import async_call_method, py3


class CursorNotReadAllDataError(Exception):
    pass


class CursorNotIterError(Exception):
    pass


class Cursor(object):
    __delegate_class__ = OriginCursor

    def __init__(self, cursor):
        self._cursor = cursor

    def __del__(self):
        if self._cursor:
            try:
                self.close()
            except: pass

    def close(self):
        if self._cursor.connection is None or not self._cursor._result or not self._cursor._result.has_next:
            self._cursor.close()
            future = platform.Future()
            future.set_result(None)
        else:
            future = async_call_method(self._cursor.close)
        return future

    def nextset(self):
        return async_call_method(self._cursor.nextset)

    def mogrify(self, query, args=None):
        return self._cursor.mogrify(query, args)

    def execute(self, query, args=None):
        return async_call_method(self._cursor.execute, query, args)

    def executemany(self, query, args):
        return async_call_method(self._cursor.executemany, query, args)

    def callproc(self, procname, args=()):
        return async_call_method(self._cursor.callproc, procname, args)

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchmany(self, size=None):
        return self._cursor.fetchmany(size)

    def fetchall(self):
        return self._cursor.fetchall()

    def scroll(self, value, mode='relative'):
        return self._cursor.scroll(value, mode)

    def __iter__(self):
        return self._cursor.__iter__()

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        "WARING: if cursor not read all data, the connection next query is error"
        if self._cursor._result and self._cursor._result.has_next:
            raise CursorNotReadAllDataError("If cursor not read all data, the connection next query is error.")
        self.close()

    if py3:
        exec("""
def __aiter__(self):
    return self._cursor.__iter__()

async def __aenter__(self):
    return self

async def __aexit__(self, exc_type, exc_val, exc_tb):
    await self.close()
        """)

setattr(OriginCursor, "__tormysql_class__", Cursor)


class DictCursor(Cursor):
    __delegate_class__ = OriginDictCursor

setattr(OriginDictCursor, "__tormysql_class__", DictCursor)


class SSCursor(Cursor):
    __delegate_class__ = OriginSSCursor

    def close(self):
        if self._cursor.connection is None:
            future = platform.Future()
            future.set_result(None)
        else:
            future = async_call_method(self._cursor.close)
        return future

    def read_next(self):
        return async_call_method(self._cursor.read_next)

    def fetchone(self):
        return async_call_method(self._cursor.fetchone)

    def fetchmany(self, size=None):
        return async_call_method(self._cursor.fetchmany, size)

    def fetchall(self):
        return async_call_method(self._cursor.fetchall)

    def scroll(self, value, mode='relative'):
        return async_call_method(self._cursor.scroll, value, mode)

    def __iter__(self):
        def next():
            future = async_call_method(self._cursor.fetchone)
            if future.done() and future._result is None:
                return None
            return future
        return iter(next, None)

    def __enter__(self):
        raise AttributeError("SSCursor not support with statement")

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise AttributeError("SSCursor not support with statement")

    if py3:
        exec("""
def __aiter__(self):
    return self

async def __anext__(self):
    result = await async_call_method(self._cursor.fetchone)
    if result is None:
        raise StopAsyncIteration()
    return result

async def __aenter__(self):
    return self

async def __aexit__(self, exc_type, exc_val, exc_tb):
    await self.close()
        """)

setattr(OriginSSCursor, "__tormysql_class__", SSCursor)


class SSDictCursor(SSCursor):
    __delegate_class__ = OriginSSDictCursor

setattr(OriginSSDictCursor, "__tormysql_class__", SSDictCursor)
