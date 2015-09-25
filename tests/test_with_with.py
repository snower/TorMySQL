#!/usr/bin/env python
# encoding: utf-8
from tornado.testing import gen_test
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
