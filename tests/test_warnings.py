# encoding: utf-8
import uuid
from tornado.testing import gen_test
from . import BaseTestCase


class TestWarnings(BaseTestCase):
    @gen_test
    def test0(self):
        connection = yield self.pool.Connection()
        warnings = yield connection.show_warnings()
        self.assertEqual(warnings, (), "No warnings")

    @gen_test
    def test1(self):
        name = uuid.uuid4().hex
        sql = 'DROP TABLE IF EXISTS test_{name}'.format(name=name)
        with (yield self.pool.Connection()) as connection:
            with connection.cursor() as cursor:
                yield cursor.execute(sql)

        warnings = yield connection.show_warnings()
        self.assertTrue(name in warnings[0][2])
