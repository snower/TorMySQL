# encoding: utf-8
from tormysql.pool import ConnectionNotFoundError
import os
import socket
from pymysql import OperationalError
from tornado.testing import gen_test
from tormysql import Connection, ConnectionPool
from maproxy.proxyserver import ProxyServer
from . import BaseTestCase


class TestConnection(BaseTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.PARAMS = dict(self.PARAMS)

        s = socket.socket()
        s.bind(('localhost', 0))
        _, self.port = s.getsockname()

        self.proxy = ProxyServer(self.PARAMS['host'], self.PARAMS['port'])
        s.close()
        self.proxy.listen(self.port)
        self.PARAMS['port'] = self.port

    def _close_proxy_sessions(self):
        for sock in self.proxy.SessionsList:
            try:
                sock.c2p_stream.close()
            except:
                pass

    def tearDown(self):
        super(BaseTestCase, self).tearDown()
        try:
            self.proxy.stop()
        except:
            pass

    @gen_test
    def test_connection_closing(self):
        connection = yield Connection(**self.PARAMS)
        cursor = connection.cursor()
        self._close_proxy_sessions()
        try:
            yield cursor.execute('SELECT 1')
        except OperationalError:
            pass
        else:
            raise AssertionError("Unexpected normal situation")

    @gen_test
    def test_pool_closing(self):
        pool = ConnectionPool(
            max_connections=int(os.getenv("MYSQL_POOL", "5")),
            idle_seconds=7200,
            **self.PARAMS
        )
        try:
            with (yield pool.Connection()) as connect:
                with connect.cursor() as cursor:
                    self._close_proxy_sessions()
                    yield cursor.execute("SELECT 1 as test")
        except (OperationalError, ConnectionNotFoundError) as e:
            pass
        else:
            raise AssertionError("Unexpected normal situation")
        finally:
            pool.close()

    @gen_test
    def test_connection_closed(self):
        self.proxy.stop()

        try:
            yield Connection(**self.PARAMS)
        except OperationalError:
            pass
        else:
            raise AssertionError("Unexpected normal situation")

    @gen_test
    def test_pool_closing(self):
        pool = ConnectionPool(
            max_connections=int(os.getenv("MYSQL_POOL", "5")),
            idle_seconds=7200,
            **self.PARAMS
        )

        try:
            self.proxy.stop()
            yield pool.Connection()
        finally:
            pool.close()
        raise AssertionError("Unexpected normal situation")
