# -*- coding: utf-8 -*-
# 17/12/11
# create by: snower

import os
try:
    import asyncio
    from tormysql.platform import use_asyncio
    use_asyncio()
except:
    asyncio = None

from tornado.ioloop import IOLoop
from tormysql.cursor import SSCursor
from tormysql.helpers import ConnectionPool
from tornado.testing import AsyncTestCase
from tornado.testing import gen_test
from tornado.test.util import unittest
from tormysql.util import py3

@unittest.skipIf(asyncio is None, "asyncio module not present")
class TestAsyncioCase(AsyncTestCase):
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
        super(TestAsyncioCase, self).setUp()
        self.pool = ConnectionPool(
            max_connections=int(os.getenv("MYSQL_POOL", 5)),
            idle_seconds=7200,
            **self.PARAMS
        )

    def tearDown(self):
        super(TestAsyncioCase, self).tearDown()
        self.pool.close()

    if py3:
        exec("""
@gen_test
async def test_execute(self):
    cursor = await self.pool.execute("select * from test limit 1")
    datas = cursor.fetchall()
    assert datas

    async with await self.pool.Connection() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT * FROM test limit 10")
            datas = cursor.fetchall()
            assert datas

    async with await self.pool.Connection() as conn:
        async with conn.cursor(SSCursor) as cursor:
            await cursor.execute("SELECT * FROM test limit 10000")
            async for data in cursor:
                assert data
        """)