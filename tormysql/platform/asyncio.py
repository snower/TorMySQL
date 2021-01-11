# -*- coding: utf-8 -*-
# 17/12/8
# create by: snower

from __future__ import absolute_import, division, print_function

from asyncio import coroutine, Future, events, Protocol, ensure_future

def current_ioloop():
    return events.get_event_loop()

try:
    from tornado.iostream import StreamClosedError
except ImportError:
    class StreamClosedError(IOError):
        def __init__(self, real_error=None):
            super(StreamClosedError, self).__init__('Stream is closed')
            self.real_error = real_error

class IOStream(Protocol):
    def __init__(self, address, bind_address):
        self._loop = None
        self._address = address
        self._bind_address = bind_address
        self._sock = None
        self._transport = None
        self._close_callback = None
        self._connect_future = None
        self._connect_ssl_future = None
        self._read_future = None
        self._read_bytes = 0
        self._closed = False

        self._read_buffer_size = 0
        self._read_buffer = bytearray()

    def closed(self):
        return self._closed

    def set_close_callback(self, callback):
        self._close_callback = callback

    def on_closed(self, exc_info=False):
        if self._connect_future:
            if exc_info:
                self._connect_future.set_exception(exc_info[1] if isinstance(exc_info, tuple) else exc_info)
            else:
                self._connect_future.set_exception(StreamClosedError(None))
            self._connect_future = None

        if self._connect_ssl_future:
            if exc_info:
                self._connect_ssl_future.set_exception(exc_info[1] if isinstance(exc_info, tuple) else exc_info)
            else:
                self._connect_ssl_future.set_exception(StreamClosedError(None))
            self._connect_ssl_future = None

        if self._read_future:
            if exc_info:
                self._read_future.set_exception(exc_info[1] if isinstance(exc_info, tuple) else exc_info)
            else:
                self._read_future.set_exception(StreamClosedError(None))
            self._read_future = None

        if self._close_callback:
            close_callback, self._close_callback = self._close_callback, None
            self._loop.call_soon(close_callback)

        self._closed = True

    def close(self, exc_info=False):
        if self._closed:
            return

        if self._transport:
            self._transport.close()
        else:
            self.on_closed(exc_info)

    @coroutine
    def _connect(self, address, server_hostname=None):
        if isinstance(address, (str, bytes)):
            self._transport, _ = yield from self._loop.create_unix_connection(lambda : self, address, sock=self._sock, server_hostname=server_hostname)
        else:
            self._transport, _ = yield from self._loop.create_connection(lambda : self, address[0], address[1], sock=self._sock, server_hostname=server_hostname, local_addr=self._bind_address)

    def connect(self, address, connect_timeout=0, server_hostname=None):
        assert self._connect_future is None, 'Already connecting'

        self._loop = current_ioloop()
        future = self._connect_future = Future(loop=self._loop)
        if connect_timeout:
            def on_timeout():
                self._loop_connect_timeout = None
                if self._connect_future:
                    self.close((None, IOError("Connect timeout"), None))

            self._loop_connect_timeout = self._loop.call_later(connect_timeout, on_timeout)

        def connected(connect_future):
            if self._loop_connect_timeout:
                self._loop_connect_timeout.cancel()
                self._loop_connect_timeout = None

            if connect_future._exception is not None:
                self.on_closed(connect_future.exception())
                self._connect_future = None
            else:
                self._connect_future = None
                future.set_result(connect_future.result())

        connect_future = ensure_future(self._connect(address, server_hostname))
        connect_future.add_done_callback(connected)
        return self._connect_future

    def connection_made(self, transport):
        self._transport = transport
        if self._connect_future is None and self._connect_ssl_future is None:
            transport.close()
        else:
            self._transport.set_write_buffer_limits(1024 * 1024 * 1024)

    def data_received(self, data):
        if self._read_buffer_size:
            self._read_buffer += data
        else:
            self._read_buffer = bytearray(data)
        self._read_buffer_size += len(data)
        if self._read_future and self._read_buffer_size >= self._read_bytes:
            future, self._read_future = self._read_future, None
            self._read_buffer, data = bytearray(), self._read_buffer
            self._read_buffer_size = 0
            self._read_bytes = 0
            future.set_result(data)

    def connection_lost(self, exc):
        self.on_closed(exc)
        self._transport = None

    def eof_received(self):
        return False

    def read_bytes(self, num_bytes):
        assert self._read_future is None, "Already reading"
        if self._closed:
            raise StreamClosedError(IOError('Already Closed'))

        future = self._read_future = Future()
        self._read_bytes = num_bytes
        if self._read_buffer_size >= self._read_bytes:
            future, self._read_future = self._read_future, None
            self._read_buffer, data = bytearray(), self._read_buffer
            self._read_buffer_size = 0
            self._read_bytes = 0
            future.set_result(data)
        return future

    def write(self, data):
        if self._closed:
            raise StreamClosedError(IOError('Already Closed'))

        self._transport.write(data)

    def start_tls(self, server_side, ssl_options=None, server_hostname=None, connect_timeout=None):
        if not self._transport or self._read_future:
            raise ValueError("IOStream is not idle; cannot convert to SSL")

        self._connect_ssl_future = connect_ssl_future = Future(loop=self._loop)
        waiter = Future(loop=self._loop)

        def on_connected(future):
            if self._loop_connect_timeout:
                self._loop_connect_timeout.cancel()
                self._loop_connect_timeout = None

            if connect_ssl_future._exception is not None:
                self.on_closed(future.exception())
                self._connect_ssl_future = None
            else:
                self._connect_ssl_future = None
                connect_ssl_future.set_result(self)
        waiter.add_done_callback(on_connected)

        if connect_timeout:
            def on_timeout():
                self._loop_connect_timeout = None
                if not waiter.done():
                    self.close((None, IOError("Connect timeout"), None))

            self._loop_connect_timeout = self._loop.call_later(connect_timeout, on_timeout)

        self._transport.pause_reading()
        sock, self._transport._sock = self._transport._sock, None
        self._transport = self._loop._make_ssl_transport(
            sock, self, ssl_options, waiter,
            server_side=False, server_hostname=server_hostname)

        return connect_ssl_future

    def makefile(self, mode):
        return self