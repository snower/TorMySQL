# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from pymysql.cursors import Cursor as OriginCursor, DictCursor as OriginDictCursor, SSCursor as OriginSSCursor, SSDictCursor as OriginSSDictCursor
from util import async_call_method

class Cursor(object):
    __delegate_class__ = OriginCursor

    def __init__(self, cursor):
        self._cursor = cursor

    def __getattr__(self, name):
        def _(*args, **kwargs):
            fun = getattr(self._cursor, name)
            return async_call_method(fun, *args, **kwargs)
        setattr(self, name, _)
        return _

setattr(OriginCursor, "__real_class__", Cursor)

class DictCursor(Cursor):
    __delegate_class__ = OriginDictCursor

setattr(OriginDictCursor, "__real_class__", DictCursor)

class SSCursor(Cursor):
    __delegate_class__ = OriginSSCursor

setattr(OriginSSCursor, "__real_class__", SSCursor)

class SSDictCursor(SSCursor):
    __delegate_class__ = OriginSSDictCursor

setattr(OriginSSDictCursor, "__real_class__", SSDictCursor)