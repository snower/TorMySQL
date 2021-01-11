# encoding: utf-8

import os
import time
import threading
import socket
import os
from tornado import gen
from tormysql.pool import ConnectionNotFoundError
from pymysql import OperationalError
from tornado.testing import gen_test
from tormysql import Connection, ConnectionPool
import sevent
from tests import BaseTestCase

class Request(object):
    def __init__(self, conn, host, port):
        self.conn = conn
        self.pconn = sevent.tcp.Socket()
        self.buffer = None
        self.connected = False

        self.conn.on("data", self.on_data)
        self.conn.on("close", self.on_close)

        self.pconn.on("connect", self.on_pconnect)
        self.pconn.on("data", self.on_pdata)
        self.pconn.on("close", self.on_pclose)

        self.pconn.connect((host, int(port)))

    def on_data(self, conn, data):
        if self.connected:
            self.pconn.write(data)
        else:
            self.buffer = data

    def on_pdata(self, conn, data):
        self.conn.write(data)

    def on_close(self, conn):
        self.pconn.end()
        try:
            TestThroughProxy.proxys.remove(self)
        except:
            pass

    def on_pclose(self, conn):
        self.conn.end()
        try:
            TestThroughProxy.proxys.remove(self)
        except:
            pass

    def on_pconnect(self, conn):
        self.connected = True
        if self.buffer:
            self.pconn.write(self.buffer)
            self.buffer = None

class TestThroughProxy(BaseTestCase):
    proxys = []

    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.PARAMS = dict(self.PARAMS)
        self.host, self.port = self.PARAMS['host'], self.PARAMS['port']

    def init_proxy(self):
        s = socket.socket()
        s.bind(('127.0.0.1', 0))
        _, self.pport = s.getsockname()
        s.close()

        def on_connect(server, conn):
            TestThroughProxy.proxys.append(Request(conn, self.host, self.port))

        self.proxy_server = sevent.tcp.Server()
        self.proxy_server.on("connection", on_connect)
        self.proxy_server.listen(("0.0.0.0", self.pport))

        self.PARAMS['port'] = self.pport
        self.PARAMS['host'] = '127.0.0.1'
        sevent.current().wakeup()

    def _close_proxy_sessions(self):
        def do_close():
            for request in TestThroughProxy.proxys:
                request.conn.end()
        sevent.current().wakeup(do_close)

    def tearDown(self):
        try:
            def do_close():
                for request in TestThroughProxy.proxys:
                    request.conn.end()
                self.proxy_server.close()
            sevent.current().wakeup(do_close)
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

        sevent.current().wakeup(self.proxy_server.close)

    @gen.coroutine
    def _execute_test_connection_closed(self):
        self.init_proxy()

        conn = yield Connection(**self.PARAMS)
        yield conn.close()

        sevent.current().wakeup(self.proxy_server.close)

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

            sevent.current().wakeup(self.proxy_server.close)

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

        sevent.current().wakeup(self.proxy_server.close)

    @gen_test
    def test(self):
        loop = sevent.instance()
        def run():
            loop.start()

        self.proxy_thread = threading.Thread(target=run)
        self.proxy_thread.setDaemon(True)
        self.proxy_thread.start()

        yield self._execute_test_connection_closing()
        yield self._execute_test_connection_closed()
        yield self._execute_test_remote_closing()
        yield self._execute_test_pool_closing()
