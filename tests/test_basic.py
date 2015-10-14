# encoding: utf-8
from tornado.testing import gen_test
from . import BaseTestCase


class TestBasic(BaseTestCase):
    @gen_test
    def test0(self):
        sql = "select * from test limit 1"
        connection = yield self.pool.Connection()
        cursor = connection.cursor()
        yield cursor.execute(sql)

        datas = cursor.fetchall()
        yield cursor.close()
        connection.close()
        assert datas
