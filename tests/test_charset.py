#!/usr/bin/env python
# encoding: utf-8
from tornado.testing import gen_test
from . import BaseTestCase


class TestLocale(BaseTestCase):
    CHARSETS = [
        'big5', 'cp850', 'latin1', 'latin2', 'ascii', 'ujis', 'sjis',
        'hebrew', 'tis620', 'euckr', 'gb2312', 'greek', 'cp1250',
        'gbk', 'latin5', 'utf8', 'cp866', 'macroman', 'cp852', 'latin7',
        'utf8mb4', 'cp1251', 'cp1256', 'cp1257', 'cp932'
    ]

    @gen_test
    def test1(self):
        for charset in self.CHARSETS:
            with (yield self.pool.Connection()) as connection:
                yield connection.set_charset(charset)

                with connection.cursor() as cursor:
                    yield cursor.execute("SHOW VARIABLES LIKE 'character_set_client'")
                    data = cursor.fetchone()
                    self.assertEqual(data[1], charset)

    @gen_test
    def test_escape(self):
        with (yield self.pool.Connection()) as connection:
            s = connection.escape(r'"')
            self.assertEqual(s, '\'\\"\'')
