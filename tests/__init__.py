#!/usr/bin/env python
# encoding: utf-8

import os
import select
from tormysql.platform import use_tornado
use_tornado()
from tormysql import ConnectionPool
from tornado.ioloop import IOLoop as BaseIOLoop
from tornado.testing import AsyncTestCase

class IOLoop(BaseIOLoop):
    @classmethod
    def configurable_default(cls):
        if hasattr(select, "epoll"):
            from tornado.platform.epoll import EPollIOLoop
            return EPollIOLoop
        if hasattr(select, "kqueue"):
            # Python 2.6+ on BSD or Mac
            from tornado.platform.kqueue import KQueueIOLoop
            return KQueueIOLoop
        from tornado.platform.select import SelectIOLoop
        return SelectIOLoop

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
        super(BaseTestCase, self).tearDown()
        if not self.pool.closed:
            self.pool.close()

    def get_new_ioloop(self):
        return IOLoop()