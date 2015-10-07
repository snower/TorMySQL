# encoding: utf-8
from .client import Client
from .cursor import Cursor, DictCursor, SSCursor, SSDictCursor
from .pool import ConnectionPool

version = "0.1.2"
version_info = (0, 1, 2)


def connect(*args, **kwargs):
    client = Client(*args, **kwargs)
    return client.connect()


Connection = connect