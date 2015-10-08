# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from .client import Client
from .cursor import Cursor, DictCursor, SSCursor, SSDictCursor
from .pool import ConnectionPool

version = "0.1.3"
version_info = (0, 1, 3)


def connect(*args, **kwargs):
    client = Client(*args, **kwargs)
    return client.connect()


Connection = connect