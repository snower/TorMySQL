#!/usr/bin/env python
# encoding: utf-8
import uuid
from tornado.testing import gen_test
from . import BaseTestCase


class TestPing(BaseTestCase):
    @gen_test
    def test1(self):
        with (yield self.pool.Connection()) as connection:
            yield connection.ping()
