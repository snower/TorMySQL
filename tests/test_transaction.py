# encoding: utf-8
import uuid
from tornado.testing import gen_test
from . import BaseTestCase


class TestTransactions(BaseTestCase):
    @gen_test
    def test0(self):
        name = str(uuid.uuid4().hex)
        connection = yield self.pool.Connection()
        cursor = connection.cursor()
        yield cursor.execute("CREATE TABLE test_{name} (id INT, data VARCHAR(100))".format(name=name))
        yield cursor.close()
        try:
            yield connection.begin()
            cursor = connection.cursor()
            yield cursor.execute("INSERT INTO test_{name} (data) VALUES ('test')".format(name=name))
            yield cursor.close()
            yield connection.rollback()

            cursor = connection.cursor()
            yield cursor.execute("SELECT COUNT(*) FROM test_{name}".format(name=name))
            data = cursor.fetchone()
            yield cursor.close()
            connection.close()
        finally:
            cursor = connection.cursor()
            yield cursor.execute("DROP TABLE test_{name}".format(name=name))
            yield cursor.close()

        self.assertEqual(data, (0,), "Transaction not working")