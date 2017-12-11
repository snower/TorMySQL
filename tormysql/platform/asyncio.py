# -*- coding: utf-8 -*-
# 17/12/8
# create by: snower

from __future__ import absolute_import, division, print_function

from asyncio import coroutine, Future, events, Protocol, ensure_future

def current_ioloop():
    return events.get_event_loop()

class StreamClosedError(IOError):
    pass

class IOStream(Protocol):
    def __init__(self, address, bind_address):
        self._loop = None
        self._address = address
        self._bind_address = bind_address
        self._sock = None
        self._transport = None
        self._close_callback = None
        self._connect_future = None
        self._read_future = None
        self._read_bytes = 0
        self._closed = False

        self._read_buffer_size = 0
        self._read_buffer = bytearray()

    def closed(self):
        return self._closed

    def set_close_callback(self, callback):
        self._close_callback = callback

    def close(self, exc_info=False):
        if self._connect_future and exc_info:
            self._connect_future.set_exception(exc_info[1])
            self._connect_future = None

        if self._close_callback:
            close_callback, self._close_callback = self._close_callback, None
            close_callback()

        if self._transport:
            self._transport.close()
            self._transport = None
        self._closed = True

    @coroutine
    def _connect(self, address, callback=None, server_hostname=None):
        if isinstance(address, (str, bytes)):
            self._transport, _ = yield from self._loop.create_unix_connection(lambda : self, address, sock=self._sock, server_hostname=server_hostname)
        else:
            self._transport, _ = yield from self._loop.create_connection(lambda : self, address[0], address[1], sock=self._sock, server_hostname=server_hostname, local_addr=self._bind_address)

    def connect(self, address, connect_timeout = 0, server_hostname = None):
        self._loop = current_ioloop()
        future = self._connect_future = Future(loop=self._loop)
        if connect_timeout:
            def timeout():
                self._loop_connect_timeout = None
                if self._connect_future:
                    self.close((None, IOError("Connect timeout"), None))

            self._loop_connect_timeout = self._loop.call_later(connect_timeout, connect_timeout)

        def connected(connect_future):
            if self._loop_connect_timeout:
                self._loop_connect_timeout.cancel()
                self._loop_connect_timeout = None

            if connect_future._exception is not None:
                future.set_exception(connect_future.exception())
            else:
                future.set_result(connect_future.result())
            self._connect_future = None

        connect_future = ensure_future(self._connect(address, None, server_hostname))
        connect_future.add_done_callback(connected)
        return self._connect_future

    def connection_made(self, transport):
        self._transport = transport
        if self._connect_future is None:
            self.close((None, StreamClosedError('Already Closed'), None))
        else:
            self._transport.set_write_buffer_limits(0)

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
        self.close(exc)

    def read_bytes(self, num_bytes):
        assert self._read_future is None, "Already reading"
        if not self._transport:
            raise StreamClosedError('Already Closed')

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
        assert isinstance(data, (bytes, bytearray))
        if not self._transport:
            raise StreamClosedError('Already Closed')

        self._transport.write(data)