# -*- coding: utf-8 -*-
# 15/2/9
# create by: snower

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from tornado import gen
from tornado import ioloop
import tormysql

pool = tormysql.ConnectionPool(
    host = "127.0.0.1",
    user = "root",
    passwd = "123456",
    db = "test",
    charset = "utf8"
)

@gen.coroutine
def test():
    sql = "select * from test limit 1"
    connection = yield pool.Connection()
    cursor = connection.cursor()
    yield cursor.execute(sql)
    datas = cursor.fetchall()
    yield cursor.close()
    connection.close()
    print datas
    yield pool.close()
    loop.stop()

def start():
    test()

loop = ioloop.IOLoop.instance()
loop.add_callback(start)
loop.start()
