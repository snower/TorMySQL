# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

'''
MySQL asynchronous client pool.
'''

import sys
import time
from collections import deque
from . import platform
from .client import Client
from . import log
from .util import py3, text_type


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

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    if py3:
        exec("""
async def __aenter__(self):
    return self

async def __aexit__(self, exc_type, exc_val, exc_tb):
    if self._connection.autocommit_mode:
        self.close()
    else:
        try:
            if exc_type:
                await self.rollback()
            else:
                await self.commit()
        except:
            exc_info = sys.exc_info()
            self.close(True)
            try:
                raise exc_info[1].with_traceback(exc_info[2])
            finally:
                exc_info = None
        else:
            self.close()
        """)

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
        self._loop = None
        self._max_connections         = kwargs.pop("max_connections", 32)
        self._idle_seconds            = kwargs.pop("idle_seconds", 7200)
        self._wait_connection_timeout = kwargs.pop("wait_connection_timeout", 8)
        self._debug_connection_used   = kwargs.pop("debug_connection_used", False)
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
        if (hasattr(connection_future, "_exc_info") and connection_future._exc_info is not None) \
                or (hasattr(connection_future, "_exception") and connection_future._exception is not None):
            future.set_exception(connection_future.exception())
        else:
            future.set_result(connection_future.result())

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
                    self._loop.call_soon(wait_future.set_exception, WaitConnectionTimeoutError(
                        "Wait connection timeout, used time %.2fs." % wait_time))
                else:
                    self._loop.call_soon(self.init_connection, wait_future)

    def init_connection(self, future = None):
        self._loop = platform.current_ioloop()
        future = future or platform.Future()
        connection = self._connection_cls(self, *self._args, **self._kwargs)
        connection.set_close_callback(self.connection_close_callback)
        connection_future = connection.connect()
        self._connections_count += 1
        self._used_connections[id(connection)] = connection
        connection_future.add_done_callback(lambda connection_future: self.connection_connected_callback(future, connection_future))

        if self._idle_seconds > 0 and not self._check_idle_callback:
            self._loop.call_later(min(self._idle_seconds, 60), self.check_idle_connections)
            self._check_idle_callback = True
        return future

    def get_connection(self):
        if self._closed:
            raise ConnectionPoolClosedError("Connection pool closed.")

        while self._connections:
            connection = self._connections.pop()
            self._used_connections[id(connection)] = connection
            connection.used_time = time.time()
            if connection.open:
                future = platform.Future()
                future.set_result(connection)
                return future

        if self._connections_count < self._max_connections:
            future = self.init_connection()
        else:
            future = platform.Future()
            self._wait_connections.append((future, time.time()))
        return future

    Connection = get_connection

    connect = get_connection

    def release_connection(self, connection):
        if self._closed:
            return connection.do_close()

        if not connection.open:
            future = platform.Future()
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
                    self._loop.call_soon(connection.do_close)
                    raise ConnectionNotFoundError("Connection not found.")
                else:
                    raise ConnectionNotUsedError("Connection is not used, you maybe close wrong connection.")

        future = platform.Future()
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
            self._loop.call_soon(wait_future.set_result, connection)
            if self._wait_connection_timeout_futures:
                self._loop.call_soon(self.do_wait_future_exception_timeout)
            return True

        if self._wait_connection_timeout_futures:
            self._loop.call_soon(self.do_wait_future_exception_timeout)
        return False

    def do_wait_future_exception_timeout(self):
        while self._wait_connection_timeout_futures:
            wait_future, wait_time = self._wait_connection_timeout_futures.popleft()
            self._loop.call_soon(wait_future.set_exception,
                                WaitConnectionTimeoutError("Wait connection timeout, used time %.2fs." % wait_time))

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
            self._loop.call_soon(self._close_future.set_result, None)
            self._close_future = None

    def close(self, timeout=None):
        self._loop = platform.current_ioloop()
        if self._closed:
            raise ConnectionPoolClosedError("Connection pool closed.")
        self._closed = True

        self._close_future = close_future = platform.Future()

        if self._used_connections:
            if timeout:
                def on_timeout():
                    if self._closed and self._close_future and not self._close_future.done():
                        close_future, self._close_future = self._close_future, None
                        close_future.set_exception(
                            ConnectionPoolUsedError("Connection pool is used, you must wait all query is finish."))
                self._loop.call_later(timeout, on_timeout)

        while len(self._wait_connections):
            future, create_time = self._wait_connections.popleft()
            wait_time = time.time() - create_time
            if wait_time >= self._wait_connection_timeout:
                self._loop.call_soon(future.set_exception, WaitConnectionTimeoutError(
                    "Wait connection timeout, used time %.2fs." % wait_time))
            else:
                self._loop.call_soon(future.set_exception,
                                              ConnectionPoolClosedError("Connection pool closed."))

        while len(self._connections):
            connection = self._connections.popleft()
            self._used_connections[id(connection)] = connection
            connection.do_close()

        if not self._connections_count:
            close_future.set_result(None)
            self._close_future = None

        return close_future

    def check_idle_connections(self):
        self._loop = platform.current_ioloop()
        now = time.time()

        while self._wait_connections:
            wait_future, create_time = self._wait_connections[0]
            wait_time = now - create_time
            if wait_time < self._wait_connection_timeout:
                break
            self._wait_connections.popleft()
            self._loop.call_soon(wait_future.set_exception, WaitConnectionTimeoutError(
                "Wait connection timeout, used time %.2fs." % wait_time))

        for connection in self._used_connections.values():
            if now - connection.used_time > (self._wait_connection_timeout * 4) ** 2:
                connection.do_close()
                if self._debug_connection_used:
                    log.get_log().error("Connection used timeout close, used time %.2fs %s %s.\n%s",
                                        now - connection.used_time, connection, self, connection.get_last_query_sql())
                else:
                    log.get_log().error("Connection used timeout close, used time %.2fs %s %s.",
                                        now - connection.used_time, connection, self)
            elif now - connection.used_time > self._wait_connection_timeout ** 2 * 2:
                if self._debug_connection_used:
                    log.get_log().warning("Connection maybe not release, used time %.2fs %s %s.\n%s",
                                          now - connection.used_time, connection, self, connection.get_last_query_sql())
                else:
                    log.get_log().warning("Connection maybe not release, used time %.2fs %s %s.",
                                          now - connection.used_time, connection, self)
            elif self._debug_connection_used:
                log.get_log().warning("Connection used time %.2fs %s %s.\n%s", now - connection.used_time, connection,
                                      self, connection.get_last_query_sql())

        next_check_time = now + self._idle_seconds
        for connection in tuple(self._connections):
            if now - connection.idle_time > self._idle_seconds:
                self.close_connection(connection)
            elif connection.idle_time + self._idle_seconds < next_check_time:
                next_check_time = connection.idle_time + self._idle_seconds
                
        if not self._closed and (self._connections or self._used_connections):
            self._loop.call_later(min(next_check_time - now, 60), self.check_idle_connections)
        else:
            self._check_idle_callback = False

    def __str__(self):
        return "%s <%s,%s>" % (
        super(ConnectionPool, self).__str__(), len(self._connections), len(self._used_connections))
