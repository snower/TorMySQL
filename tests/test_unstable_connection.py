# encoding: utf-8
import socket
import os
from tornado import gen
from tornado.ioloop import IOLoop
from tormysql.pool import ConnectionNotFoundError
from pymysql import OperationalError
from tornado.testing import gen_test
from tormysql import Connection, ConnectionPool
from maproxy.proxyserver import ProxyServer
from maproxy.session import SessionFactory
from tests import BaseTestCase


class TestThroughProxy(BaseTestCase):
    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.PARAMS = dict(self.PARAMS)

        s = socket.socket()
        s.bind(('127.0.0.1', 0))

        self.host, self.port = self.PARAMS['host'], self.PARAMS['port']
        _, self.pport = s.getsockname()
        s.close()

    def init_proxy(self):
        self.proxy = ProxyServer(
            self.host,
            self.port,
            session_factory=SessionFactory(),
        )
        self.PARAMS['port'] = self.pport
        self.PARAMS['host'] = '127.0.0.1'

        self.proxy.listen(self.pport)

    def _close_proxy_sessions(self):
        for sock in self.proxy.SessionsList:
            try:
                sock.c2p_stream.close()
            except:
                pass

    def tearDown(self):
        try:
            self.proxy.stop()
        except:
            pass
        super(BaseTestCase, self).tearDown()

    @gen.coroutine
    def _execute_test_connection_closing(self):
        self.init_proxy()
        connection = yield Connection(**self.PARAMS)
        cursor = connection.cursor()
        self._close_proxy_sessions()
        try:
            yield cursor.execute('SELECT 1')
            yield cursor.close()
        except OperationalError:
            pass
        else:
            raise AssertionError("Unexpected normal situation")
        self.proxy.stop()

    @gen.coroutine
    def _execute_test_connection_closed(self):
        self.init_proxy()
        conn = yield Connection(**self.PARAMS)
        yield conn.close()
        self.proxy.stop()

        try:
            yield Connection(**self.PARAMS)
        except OperationalError:
            pass
        else:
            raise AssertionError("Unexpected normal situation")

    @gen.coroutine
    def _execute_test_remote_closing(self):
        self.init_proxy()
        pool = ConnectionPool(
            max_connections=int(os.getenv("MYSQL_POOL", "5")),
            idle_seconds=7200,
            **self.PARAMS
        )

        try:
            conn = yield pool.Connection()
            yield conn.do_close()
            self.proxy.stop()
            yield pool.Connection()
        except OperationalError:
            pass
        else:
            raise AssertionError("Unexpected normal situation")
        finally:
            yield pool.close()

    @gen.coroutine
    def _execute_test_pool_closing(self):
        self.init_proxy()
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
            yield pool.close()
            self.proxy.stop()

    @gen_test
    def test(self):
        yield self._execute_test_connection_closing()
        yield self._execute_test_connection_closed()
        yield self._execute_test_remote_closing()
        yield self._execute_test_pool_closing()
