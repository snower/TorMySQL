# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from collections import deque
from tornado.concurrent import TracebackFuture
from tornado.ioloop import IOLoop
from .client import Client

class ConnectionPoolClosedError(Exception):pass
class ConnectionNotFoundError(Exception):pass
class ConnectionNotUsedError(Exception):pass

class Connection(Client):
    def __init__(self, pool, *args, **kwargs):
        self._pool = pool
        super(Connection, self).__init__(*args, **kwargs)

    def close(self):
        return self._pool.release_connection(self)

    def on_close(self):
        if self._closed:return
        self._closed = True
        self._pool.close_connection(self)

    def do_close(self):
        return super(Connection, self).close()


class ConnectionPool(object):
    def __init__(self, *args, **kwargs):
        self._max_connections = kwargs.pop("max_connections") if "max_connections" in kwargs else 1
        self._args = args
        self._kwargs = kwargs
        self._connections = deque()
        self._used_connections = deque()
        self._connections_count = 0
        self._wait_connections = deque()
        self._closed = False
        self._close_results = []
        self._close_future = None
        self._close_future_count = 0

    def init_connection(self, callback):
        def _(connection_future):
            if connection_future._exc_info is None:
                connection = connection_future._result
                callback(True, connection)
            else:
                callback(False, connection_future._exc_info)
        connection = Connection(self, *self._args, **self._kwargs)
        connection_future = connection.connect()
        self._connections_count +=1
        IOLoop.current().add_future(connection_future, _)

    def get_connection(self):
        future = TracebackFuture()
        if self._closed:
            future.set_exception(ConnectionPoolClosedError())
            return future

        if not self._connections:
            if self._connections_count < self._max_connections:
                def _(succed, result):
                    if succed:
                        self._used_connections.append(result)
                        future.set_result(result)
                    else:
                        future.set_exc_info(result)
                self.init_connection(_)
            else:
                self._wait_connections.append(future)
        else:
            connection = self._connections.popleft()
            self._used_connections.append(connection)
            future.set_result(connection)
        return future

    Connection = get_connection

    def release_connection(self, connection):
        if not self._closed and self._wait_connections:
            future = self._wait_connections.popleft()
            IOLoop.current().add_callback(lambda :future.set_result(connection))
        else:
            try:
                self._used_connections.remove(connection)
                if self._closed:
                    self.do_close_connection(connection)
                else:
                    self._connections.append(connection)
            except ValueError:
                if connection not in self._connections:
                    connection.do_close()
                    raise ConnectionNotFoundError()
                else:
                    raise ConnectionNotUsedError()

    def close_connection(self, connection):
        try:
            self._used_connections.remove(connection)
            self._connections_count -= 1
        except ValueError:
            try:
                self._connections.remove(connection)
                self._connections_count -= 1
            except ValueError:
                pass

    def _close_connection_callback(self, future):
        if future._exc_info is None:
            self._close_results.append(future._result)
            if len(self._close_results) == self._close_future_count:
                self._close_future.set_result(self._close_results)
        else:
            self._close_future.set_exc_info(future._exc_info)

    def do_close_connection(self, connection):
        future = connection.do_close()
        future.add_done_callback(self._close_connection_callback)

    def close(self):
        if not self._closed:
            self._closed = True
            self._close_future = TracebackFuture()
            self._close_future_count = len(self._used_connections) + len(self._connections)

            while len(self._wait_connections):
                future = self._wait_connections.popleft()
                IOLoop.current().add_callback(lambda :future.set_exception(ConnectionPoolClosedError()))

            while len(self._connections):
                connection = self._connections.popleft()
                self.do_close_connection(connection)
        return self._close_future