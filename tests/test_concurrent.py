#!/usr/bin/env python
# encoding: utf-8
import os
from tornado.gen import coroutine
from tornado.testing import gen_test
from . import BaseTestCase


class TestWithWith(BaseTestCase):
    @gen_test
    def test1(self):
        yield [self._exec() for _ in range(2)]

    @coroutine
    def _exec(self):
        sql = "select sleep(1)"
        with (yield self.pool.Connection()) as connection:
            with connection.cursor() as cursor:
                yield cursor.execute(sql)
                datas = cursor.fetchall()