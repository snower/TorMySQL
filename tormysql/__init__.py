# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from pymysql import *
from .client import Client
from .cursor import Cursor, DictCursor, SSCursor, SSDictCursor
from .pool import ConnectionPool

version = "0.1.2"
version_info = (0, 1, 2)


def Connection(*args, **kwargs):
    client = Client(*args, **kwargs)
    return client.connect()


connect = Connection