# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from tornado.ioloop import IOLoop
from tornado.concurrent import TracebackFuture
from .util import async_call_method
from .connections import Connection
from .cursor import Cursor

class Client(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._connection = None
        self._closed = False

        if "cursorclass" in kwargs and issubclass(kwargs["cursorclass"], Cursor):
            kwargs["cursorclass"] = kwargs["cursorclass"].__delegate_class__

    def connect(self):
        future = TracebackFuture()
        def _(connection_future):
            if connection_future._exception is None and connection_future._exc_info is None:
                self._connection = connection_future._result
                self._connection.set_close_callback(self.on_close)
                future.set_result(self)
            else:
                future.set_exc_info(connection_future._exc_info) if connection_future._exc_info else future.set_exc_info(connection_future._exception)
        connection_future = async_call_method(Connection, *self._args, **self._kwargs)
        IOLoop.current().add_future(connection_future, _)
        return future

    def on_close(self):
        self._closed = True

    def close(self):
        if self._closed:return
        return async_call_method(self._connection.close)

    def autocommit(self, value):
        return async_call_method(self._connection.autocommit, value)

    def commit(self):
        return async_call_method(self._connection.commit)

    def rollback(self):
        return async_call_method(self._connection.rollback)

    def select_db(self, db):
        return async_call_method(self._connection.select_db, db)

    def escape(self, obj):
        return self._connection.escape(obj)

    def literal(self, obj):
        return self._connection.literal(obj)

    def escape_string(self, s):
        return self._connection.escape_string(s)

    def cursor(self, cursor=None):
        future = TracebackFuture()
        def _(cursor_future):
            if cursor_future._exception is None and cursor_future._exc_info is None:
                c = cursor_future._result
                if cursor:
                    c = cursor(c)
                else:
                    c = c.__tormysql_class__(c)
                future.set_result(c)
            else:
                future.set_exc_info(cursor_future._exc_info) if cursor_future._exc_info else future.set_exc_info(cursor_future._exception)
        cursor_future = async_call_method(self._connection.cursor, cursor.__delegate_class__ if cursor and issubclass(cursor,Cursor) else cursor)
        IOLoop.current().add_future(cursor_future, _)
        return future

    def query(self, sql, unbuffered=False):
        return async_call_method(self._connection.query, sql, unbuffered)

    def next_result(self):
        return async_call_method(self._connection.next_result)

    def affected_rows(self):
        return async_call_method(self._connection.affected_rows)

    def kill(self, thread_id):
        return async_call_method(self._connection.kill, thread_id)

    def ping(self, reconnect=True):
        return async_call_method(self._connection.ping, reconnect)

    def set_charset(self, charset):
        return async_call_method(self._connection.set_charset, charset)

    def insert_id(self):
        return self._connection.insert_id()