# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from pymysql import *
from client import Client
from cursor import Cursor, DictCursor, SSCursor, SSDictCursor
from pool import ConnectionPool

def Connection(*args, **kwargs):
    client = Client(*args, **kwargs)
    return client.connect()

connect = Connection