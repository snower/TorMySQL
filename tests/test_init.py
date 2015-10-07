# encoding: utf-8
from tornado.testing import gen_test
from tormysql import Connection
from . import BaseTestCase


class TestInit(BaseTestCase):
    @gen_test
    def test0(self):
        connection = yield Connection(**self.PARAMS)
        cursor = connection.cursor()
        yield cursor.execute('SELECT 1')

        datas = cursor.fetchall()
        yield cursor.close()
        connection.close()
        assert datas
