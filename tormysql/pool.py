# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

'''
MySQL asynchronous client pool.
'''

import time
from collections import deque
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from pymysql._compat import text_type
from .client import Client
from . import log


class ConnectionPoolClosedError(Exception):
    pass


class ConnectionPoolUsedError(Exception):
    pass


class ConnectionNotFoundError(Exception):
    pass


class ConnectionNotUsedError(Exception):
    pass


class ConnectionUsedError(Exception):
    pass

class WaitConnectionTimeoutError(Exception):
    pass

class Connection(Client):
    def __init__(self, pool, *args, **kwargs):
        self._pool = pool
        self.idle_time = time.time()
        self.used_time = time.time()
        super(Connection, self).__init__(*args, **kwargs)

    def close(self, remote_close=False):
        if remote_close:
            return self.do_close()
        return self._pool.release_connection(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()

    def __del__(self):
        try:
            self.close()
        except ConnectionNotUsedError:
            pass

    def do_close(self):
        return super(Connection, self).close()


class RecordQueryConnection(Connection):
    def __init__(self, *args, **kwargs):
        super(RecordQueryConnection, self).__init__(*args, **kwargs)

        self._last_query_sql = ""

    def connect(self):
        future = super(RecordQueryConnection, self).connect()

        origin_query = self._connection.query
        def query(sql, unbuffered=False):
            self._last_query_sql = sql
            return origin_query(sql, unbuffered)
        self._connection.query = query

        return future

    def query(self, sql, unbuffered=False):
        self._last_query_sql = sql
        return super(RecordQueryConnection, self).query(sql, unbuffered)

    def get_last_query_sql(self):
        if isinstance(self._last_query_sql, text_type):
            return self._last_query_sql.encode("utf-8")
        return self._last_query_sql


class ConnectionPool(object):
    def __init__(self, *args, **kwargs):
        self._max_connections = kwargs.pop("max_connections") if "max_connections" in kwargs else 32
        self._idle_seconds = kwargs.pop("idle_seconds") if "idle_seconds" in kwargs else 7200
        self._wait_connection_timeout = kwargs.pop("wait_connection_timeout") if "wait_connection_timeout" in kwargs else 8
        self._debug_connection_used = kwargs.pop("debug_connection_used") if "debug_connection_used" in kwargs else False
        if self._debug_connection_used:
            self._connection_cls = RecordQueryConnection
        else:
            self._connection_cls = Connection
        self._args = args
        self._kwargs = kwargs
        self._connections = deque(maxlen = self._max_connections)
        self._used_connections = {}
        self._connections_count = 0
        self._wait_connections = deque()
        self._wait_connection_timeout_futures = deque()
        self._closed = False
        self._close_future = None
        self._check_idle_callback = False

    @property
    def closed(self):
        return self._closed

    def connection_connected_callback(self, future, connection_future):
        if connection_future._exc_info is None:
            future.set_result(connection_future.result())
        else:
            future.set_exc_info(connection_future.exc_info())

            while self._wait_connections and self._connections:
                connection = self._connections.pop()
                if connection.open:
                    if self.continue_next_wait(connection):
                        self._used_connections[id(connection)] = connection
                    else:
                        self._connections.append(connection)
                        break

            if self._wait_connections and self._connections_count - 1 < self._max_connections:
                wait_future, create_time = self._wait_connections.popleft()
                wait_time = time.time() - create_time
                if wait_time >= self._wait_connection_timeout:
                    IOLoop.current().add_callback(wait_future.set_exception, WaitConnectionTimeoutError("Wait connection timeout, used time %.2fs." % wait_time))
                else:
                    IOLoop.current().add_callback(self.init_connection, wait_future)

    def init_connection(self, future):
        connection = self._connection_cls(self, *self._args, **self._kwargs)
        connection.set_close_callback(self.connection_close_callback)
        connection_future = connection.connect()
        self._connections_count += 1
        self._used_connections[id(connection)] = connection
        IOLoop.current().add_future(connection_future, lambda connection_future: self.connection_connected_callback(future, connection_future))

        if self._idle_seconds > 0 and not self._check_idle_callback:
            IOLoop.current().add_timeout(time.time() + min(self._idle_seconds, 60), self.check_idle_connections)
            self._check_idle_callback = True

    def get_connection(self):
        if self._closed:
            raise ConnectionPoolClosedError("Connection pool closed.")

        future = Future()
        while self._connections:
            connection = self._connections.pop()
            self._used_connections[id(connection)] = connection
            connection.used_time = time.time()
            if connection.open:
                future.set_result(connection)
                return future

        if self._connections_count < self._max_connections:
            self.init_connection(future)
        else:
            self._wait_connections.append((future, time.time()))
        return future

    Connection = get_connection
    connect = get_connection

    def release_connection(self, connection):
        if self._closed:
            return connection.do_close()

        if not connection.open:
            future = Future()
            future.set_result(None)
            return future

        if self.continue_next_wait(connection):
            while self._wait_connections and self._connections:
                connection = self._connections.pop()
                if connection.open:
                    if self.continue_next_wait(connection):
                        self._used_connections[id(connection)] = connection
                    else:
                        self._connections.append(connection)
                        break
        else:
            try:
                del self._used_connections[id(connection)]
                self._connections.append(connection)
                connection.idle_time = time.time()
            except KeyError:
                if connection not in self._connections:
                    IOLoop.current().add_callback(connection.do_close)
                    raise ConnectionNotFoundError("Connection not found.")
                else:
                    raise ConnectionNotUsedError("Connection is not used, you maybe close wrong connection.")

        future = Future()
        future.set_result(None)
        return future

    def continue_next_wait(self, connection):
        now = time.time()
        while self._wait_connections:
            wait_future, create_time = self._wait_connections.popleft()
            wait_time = now - create_time
            if wait_time >= self._wait_connection_timeout:
                self._wait_connection_timeout_futures.append((wait_future, wait_time))
                continue
            connection.used_time = now
            IOLoop.current().add_callback(wait_future.set_result, connection)
            if self._wait_connection_timeout_futures:
                IOLoop.current().add_callback(self.do_wait_future_exception_timeout)
            return True

        if self._wait_connection_timeout_futures:
            IOLoop.current().add_callback(self.do_wait_future_exception_timeout)
        return False

    def do_wait_future_exception_timeout(self):
        ioloop = IOLoop.current()
        while self._wait_connection_timeout_futures:
            wait_future, wait_time = self._wait_connection_timeout_futures.popleft()
            ioloop.add_callback(wait_future.set_exception, WaitConnectionTimeoutError("Wait connection timeout, used time %.2fs." % wait_time))

    def close_connection(self, connection):
        try:
            self._connections.remove(connection)
            self._used_connections[id(connection)] = connection
            return connection.do_close()
        except ValueError:
            raise ConnectionUsedError("Connection is used, you can not close it.")

    def connection_close_callback(self, connection):
        try:
            del self._used_connections[id(connection)]
            self._connections_count -= 1
        except KeyError:
            try:
                self._connections.remove(connection)
                self._connections_count -= 1
            except ValueError:
                log.get_log().warning("Close unknown Connection %s.", connection)
        if self._close_future and not self._used_connections and not self._connections:
            IOLoop.current().add_callback(self._close_future.set_result, None)
            self._close_future = None

    def close(self, timeout = None):
        if self._closed:
            raise ConnectionPoolClosedError("Connection pool closed.")

        self._closed = True
        self._close_future = close_future = Future()

        if self._used_connections:
            if timeout:
                def on_timeout():
                    if self._closed and self._close_future and not self._close_future.done():
                        close_future, self._close_future = self._close_future, None
                        close_future.set_exception(ConnectionPoolUsedError("Connection pool is used, you must wait all query is finish."))
                IOLoop.current().add_timeout(time.time() + timeout, on_timeout)

        while len(self._wait_connections):
            future, create_time = self._wait_connections.popleft()
            wait_time = time.time() - create_time
            if wait_time >= self._wait_connection_timeout:
                IOLoop.current().add_callback(future.set_exception, WaitConnectionTimeoutError("Wait connection timeout, used time %.2fs." % wait_time))
            else:
                IOLoop.current().add_callback(future.set_exception, ConnectionPoolClosedError("Connection pool closed."))

        while len(self._connections):
            connection = self._connections.popleft()
            self._used_connections[id(connection)] = connection
            connection.do_close()
        return close_future

    def check_idle_connections(self):
        now = time.time()

        while self._wait_connections:
            wait_future, create_time = self._wait_connections[0]
            wait_time = now - create_time
            if wait_time < self._wait_connection_timeout:
                break
            self._wait_connections.popleft()
            IOLoop.current().add_callback(wait_future.set_exception, WaitConnectionTimeoutError("Wait connection timeout, used time %.2fs." % wait_time))

        for connection in self._used_connections.values():
            if now - connection.used_time > (self._wait_connection_timeout * 4) ** 2:
                connection.do_close()
                if self._debug_connection_used:
                    log.get_log().error("Connection used timeout close, used time %.2fs %s %s.\n%s", now - connection.used_time, connection, self, connection.get_last_query_sql())
                else:
                    log.get_log().error("Connection used timeout close, used time %.2fs %s %s.", now - connection.used_time, connection, self)
            elif now - connection.used_time > self._wait_connection_timeout ** 2 * 2:
                if self._debug_connection_used:
                    log.get_log().warning("Connection maybe not release, used time %.2fs %s %s.\n%s", now - connection.used_time, connection, self, connection.get_last_query_sql())
                else:
                    log.get_log().warning("Connection maybe not release, used time %.2fs %s %s.", now - connection.used_time, connection, self)
            elif self._debug_connection_used:
                log.get_log().warning("Connection used time %.2fs %s %s.\n%s", now - connection.used_time, connection, self, connection.get_last_query_sql())

        next_check_time = now + self._idle_seconds
        for connection in tuple(self._connections):
            if now - connection.idle_time > self._idle_seconds:
                self.close_connection(connection)
            elif connection.idle_time + self._idle_seconds < next_check_time:
                next_check_time = connection.idle_time + self._idle_seconds
                
        if not self._closed and (self._connections or self._used_connections):
            IOLoop.current().add_timeout(min(next_check_time, now + 60), self.check_idle_connections)
        else:
            self._check_idle_callback = False

    def __str__(self):
        return "%s <%s,%s>" % (super(ConnectionPool, self).__str__(), len(self._connections), len(self._used_connections))
