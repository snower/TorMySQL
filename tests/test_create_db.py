#!/usr/bin/env python
# encoding: utf-8
import uuid
from tornado.testing import gen_test
from . import BaseTestCase


class TestCreateDB(BaseTestCase):
    @gen_test
    def test1(self):
        name = "test_{0}".format(uuid.uuid4().hex)

        with (yield self.pool.Connection()) as connection:
            yield connection.begin()
            try:
                with connection.cursor() as cursor:
                    yield cursor.execute("CREATE DATABASE {0}".format(name))

                yield connection.select_db(name)

                with connection.cursor() as cursor:
                    yield cursor.execute('SHOW TABLES')
                    data = cursor.fetchall()
                    self.assertEqual(data, tuple())
            finally:
                yield connection.rollback()
