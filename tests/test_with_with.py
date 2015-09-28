#!/usr/bin/env python
# encoding: utf-8
import os
from tornado.testing import gen_test
from mytor.cursor import SSDictCursor
from . import BaseTestCase


class TestWithWith(BaseTestCase):
    @gen_test
    def test1(self):
        sql = "select * from user limit 1"
        with (yield self.pool.Connection()) as connection:
            with connection.cursor() as cursor:
                yield cursor.execute(sql)
                datas = cursor.fetchall()
                print (datas)


class TestAsyncCursor(BaseTestCase):
    PARAMS = dict(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        user=os.getenv("MYSQL_USER", "root"),
        passwd=os.getenv("MYSQL_PASSWD", ""),
        db=os.getenv("MYSQL_DB", "mysql"),
        charset=os.getenv("MYSQL_CHARSET", "utf8"),
        no_delay=True,
        sql_mode="REAL_AS_FLOAT",
        init_command="SET max_join_size=DEFAULT",
        cursorclass=SSDictCursor
    )

    @gen_test
    def test1(self):
        sql = "select 1 as test"
        with (yield self.pool.Connection()) as connection:
            with connection.cursor() as cursor:
                yield cursor.execute(sql)
                result = yield cursor.fetchone()
                self.assertTrue('test' in result)
                self.assertEqual(result['test'], 1)
                self.assertEqual(result.test, 1)
