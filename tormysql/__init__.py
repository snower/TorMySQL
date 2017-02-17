'''
TorMySQL: presents a Tornado Future-based API and greenlet for non-blocking access to MySQL.

The MIT License (MIT)

Copyright (c) 2014, 2015 TorMySQL contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

from .client import Client
from .cursor import Cursor, DictCursor, SSCursor, SSDictCursor
from .pool import ConnectionPool
from .cursor import CursorNotReadAllDataError, CursorNotIterError
from .pool import ConnectionPoolClosedError, ConnectionPoolUsedError, ConnectionNotFoundError, ConnectionNotUsedError, ConnectionUsedError, WaitConnectionTimeoutError
from .log import set_log
from . import helpers

version = "0.3.2"
version_info = (0, 3, 2)


def connect(*args, **kwargs):
    client = Client(*args, **kwargs)
    return client.connect()


Connection = connect