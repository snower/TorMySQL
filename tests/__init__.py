#!/usr/bin/env python
# encoding: utf-8

import os
from tornado.ioloop import IOLoop
from tormysql import ConnectionPool
from tornado.testing import AsyncTestCase

class BaseTestCase(AsyncTestCase):
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
        super(BaseTestCase, self).setUp()
        self.pool = ConnectionPool(
            max_connections=int(os.getenv("MYSQL_POOL", 5)),
            idle_seconds=7200,
            **self.PARAMS
        )

    def tearDown(self):
        if not self.pool.closed:
            self.pool.close()
        super(BaseTestCase, self).tearDown()

    def get_new_ioloop(self):
        return IOLoop.current()