# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower
from tornado.gen import coroutine, Return
from .util import async_call_method
from .connections import Connection
from .cursor import Cursor


class Client(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._connection = None
        self._closed = False
        self._close_callback = None

        if "cursorclass" in kwargs and issubclass(kwargs["cursorclass"], Cursor):
            if hasattr(kwargs["cursorclass"], '__delegate_class__') and kwargs["cursorclass"].__delegate_class__:
                kwargs["cursorclass"] = kwargs["cursorclass"].__delegate_class__

    @coroutine
    def connect(self):
        try:
            self._connection = yield async_call_method(Connection, *self._args, **self._kwargs)
        except Exception as e:
            self.on_close(e)
            raise
        else:
            self._connection.set_close_callback(self.on_close)
            raise Return(self)

    def on_close(self, reason=None):
        self._closed = True
        if self._close_callback:
            self._close_callback(self)
            self._close_callback = None

    def set_close_callback(self, callback):
        self._close_callback = callback

    def close(self):
        if self._closed:
            return
        return async_call_method(self._connection.close)

    def autocommit(self, value):
        return async_call_method(self._connection.autocommit, value)

    def begin(self):
        return async_call_method(self._connection.begin)

    def commit(self):
        return async_call_method(self._connection.commit)

    def rollback(self):
        return async_call_method(self._connection.rollback)

    def show_warnings(self):
        return async_call_method(self._connection.show_warnings)

    def select_db(self, db):
        return async_call_method(self._connection.select_db, db)

    def cursor(self, cursor_cls=None):
        cursor = self._connection.cursor(
            cursor_cls.__delegate_class__ if cursor_cls and issubclass(cursor_cls, Cursor) else cursor_cls
        )

        if cursor_cls:
            return cursor_cls(cursor)
        else:
            return cursor.__mytor_class__(cursor)

    def query(self, sql, unbuffered=False):
        return async_call_method(self._connection.query, sql, unbuffered)

    def next_result(self):
        return async_call_method(self._connection.next_result)

    def kill(self, thread_id):
        return async_call_method(self._connection.kill, thread_id)

    def ping(self, reconnect=True):
        return async_call_method(self._connection.ping, reconnect)

    def set_charset(self, charset):
        return async_call_method(self._connection.set_charset, charset)

    def __getattr__(self, name):
        return getattr(self._connection, name)
