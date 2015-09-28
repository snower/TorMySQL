# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower
from tornado.ioloop import IOLoop
from tornado.concurrent import TracebackFuture
from pymysql.cursors import (
    Cursor as OriginCursor, DictCursor as OriginDictCursor,
    SSCursor as OriginSSCursor, SSDictCursor as OriginSSDictCursor
)
from .util import async_call_method


class Cursor(object):
    __delegate_class__ = OriginCursor

    def __init__(self, cursor):
        self._cursor = cursor

    def __del__(self):
        if self._cursor:
            future = async_call_method(self._cursor.close)
            def do_close(future):
                self._cursor = None
            future.add_done_callback(do_close)

    def close(self):
        if self._cursor is None:
            future = TracebackFuture()
            future.set_result(None)
            return future
        future = async_call_method(self._cursor.close)

        def do_close(future):
            self._cursor = None
        future.add_done_callback(do_close)
        return future

    def execute(self, query, args=None):
        return async_call_method(self._cursor.execute, query, args)

    def executemany(self, query, args):
        return async_call_method(self._cursor.executemany, query, args)

    def callproc(self, procname, args=()):
        return async_call_method(self._cursor.procname, procname, args)

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

    def __exit__(self, *args):
        IOLoop.current().add_callback(self.close)


setattr(OriginCursor, "__tormysql_class__", Cursor)


class DictCursor(Cursor):
    __delegate_class__ = OriginDictCursor


setattr(OriginDictCursor, "__tormysql_class__", DictCursor)


class SSCursor(Cursor):
    __delegate_class__ = OriginSSCursor

    def read_next(self):
        return async_call_method(self._cursor.read_next)

    def fetchone(self):
        return async_call_method(self._cursor.fetchone)

    def fetchall(self):
        return async_call_method(self._cursor.fetchall)

    def __iter__(self):
        return self.fetchall()

    def fetchmany(self, size=None):
        return async_call_method(self._cursor.fetchmany, size)

    def scroll(self, value, mode='relative'):
        return async_call_method(self._cursor.scroll, value, mode)


setattr(OriginSSCursor, "__tormysql_class__", SSCursor)


class SSDictCursor(SSCursor):
    __delegate_class__ = OriginSSDictCursor

setattr(OriginSSDictCursor, "__tormysql_class__", SSDictCursor)
