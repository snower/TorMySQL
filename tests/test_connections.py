# encoding: utf-8
from tormysql import Connection
from tornado.testing import gen_test
from . import BaseTestCase


class TestConnections(BaseTestCase):
    @gen_test
    def test_del(self):
        connection = yield Connection(**self.PARAMS)
        connection.__del__()
