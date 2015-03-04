# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from pymysql import *
from .client import Client
from .cursor import Cursor, DictCursor, SSCursor, SSDictCursor
from .pool import ConnectionPool

version = "0.0.8"
version_info = (0,0,8)

def Connection(*args, **kwargs):
    client = Client(*args, **kwargs)
    return client.connect()

connect = Connection