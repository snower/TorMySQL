#!/usr/bin/env python
# encoding: utf-8

import os
from tornado import gen
from tornado.testing import gen_test
from tormysql.cursor import SSDictCursor
from . import BaseTestCase
from tormysql import ConnectionPool

class TestWithWith(BaseTestCase):
    @gen.coroutine
    def _execute_test1(self):
        sql = "select * from test limit 1"
        with (yield self.pool.Connection()) as connection:
            with connection.cursor() as cursor:
                yield cursor.execute(sql)
                datas = cursor.fetchall()
                self.assertTrue(bool(datas))

        yield self.pool.close()

    def init_params(self):
        self.PARAMS = dict(
            host=os.getenv("MYSQL_HOST", "127.0.0.1"),
            user=os.getenv("MYSQL_USER", "root"),
            passwd=os.getenv("MYSQL_PASSWD", ""),
            db=os.getenv("MYSQL_DB", "test"),
            charset=os.getenv("MYSQL_CHARSET", "utf8"),
            no_delay=True,
            sql_mode="REAL_AS_FLOAT",
            init_command="SET max_join_size=DEFAULT",
            cursorclass=SSDictCursor
        )

        self.pool = ConnectionPool(
            max_connections=int(os.getenv("MYSQL_POOL", 5)),
            idle_seconds=7200,
            **self.PARAMS
        )

    @gen.coroutine
    def _execute_test2(self):
        self.init_params()
        sql = "select 1 as test"
        with (yield self.pool.Connection()) as connection:
            cursor = connection.cursor()
            yield cursor.execute(sql)
            result = yield cursor.fetchone()
            yield cursor.close()
            self.assertTrue('test' in result)
            self.assertEqual(result['test'], 1)

    @gen_test
    def test(self):
        yield self._execute_test1()
        yield self._execute_test2()