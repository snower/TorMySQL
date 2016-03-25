# -*- coding: utf-8 -*-
# 16/3/25
# create by: snower

import os
from tormysql.helpers import ConnectionPool
from tornado.testing import AsyncTestCase
from tornado.testing import gen_test

class TestHelpersCase(AsyncTestCase):
    PARAMS = dict(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        passwd=os.getenv("MYSQL_PASSWD", ""),
        db=os.getenv("MYSQL_DB", "test"),
        charset=os.getenv("MYSQL_CHARSET", "utf8"),
        no_delay=True,
        sql_mode="REAL_AS_FLOAT",
        init_command="SET max_join_size=DEFAULT"
    )

    def setUp(self):
        super(TestHelpersCase, self).setUp()
        self.pool = ConnectionPool(
            max_connections=int(os.getenv("MYSQL_POOL", 5)),
            idle_seconds=7200,
            **self.PARAMS
        )

    def tearDown(self):
        super(TestHelpersCase, self).tearDown()
        self.pool.close()

    @gen_test
    def test_execute(self):
        sql = "select 1 as test"
        cursor = yield self.pool.execute(sql)
        result = cursor.fetchone()
        self.assertTrue(result == (1,))

    @gen_test
    def test_tx(self):
        sql = "select 1 as test"
        tx = yield self.pool.begin()
        cursor = yield tx.execute(sql)
        yield tx.commit()
        result = cursor.fetchone()
        self.assertTrue(result == (1,))