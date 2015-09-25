#!/usr/bin/env python
# encoding: utf-8
import os
from tormysql import ConnectionPool
from tornado.testing import AsyncTestCase


class BaseTestCase(AsyncTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.pool = ConnectionPool(
            max_connections=int(os.getenv("MYSQL_POOL", "5")),
            idle_seconds=7200,
            host=os.getenv("MYSQL_HOST", "127.0.0.1"),
            user=os.getenv("MYSQL_USER", "root"),
            passwd=os.getenv("MYSQL_PASSWD", ""),
            db=os.getenv("MYSQL_DB", "mysql"),
            charset=os.getenv("MYSQL_CHARSET", "utf8"),
        )

    def tearDown(self):
        super(BaseTestCase, self).tearDown()
        self.pool.close()
