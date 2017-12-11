# -*- coding: utf-8 -*-
# 17/12/8
# create by: snower

from __future__ import absolute_import, division, print_function

import sys
import socket
import errno
from tornado.iostream import IOStream as BaseIOStream, StreamClosedError, _ERRNO_WOULDBLOCK
from tornado.concurrent import Future
from tornado.gen import coroutine
from tornado.ioloop import IOLoop

if sys.version_info[0] >= 3:
    import io
    StringIO = io.BytesIO
else:
    import cStringIO
    StringIO = cStringIO.StringIO

def current_ioloop():
    return IOLoop.current()


class IOStream(BaseIOStream):
    def __init__(self, address, bind_address, *args, **kwargs):
        socket = self.init_socket(address, bind_address)

        super(IOStream, self).__init__(socket, *args, **kwargs)

    def init_socket(self, address, bind_address):
        if not isinstance(address, tuple):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            if bind_address is not None:
                sock.bind((bind_address, 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return sock

    def connect(self, address, connect_timeout = 0, server_hostname = None):
        future = Future()
        if connect_timeout:
            def timeout():
                self._loop_connect_timeout = None
                if not self._connecting:
                    self.close((None, IOError("Connect timeout"), None))

            self._loop_connect_timeout = self.io_loop.call_later(connect_timeout, timeout)

        def connected(connect_future):
            if self._loop_connect_timeout:
                self.io_loop.remove_timeout(self._loop_connect_timeout)
                self._loop_connect_timeout = None

            if (hasattr(connect_future, "_exc_info") and connect_future._exc_info is not None) \
                    or (hasattr(connect_future, "_exception") and connect_future._exception is not None):
                future.set_exception(connect_future.exception())
            else:
                future.set_result(connect_future.result())

        connect_future = super(IOStream, self).connect(address, None, server_hostname)
        connect_future.add_done_callback(connected)
        return future

    def _handle_events(self, fd, events):
        if self._closed:
            return
        try:
            if self._connecting:
                self._handle_connect()
            if self._closed:
                return
            if events & self.io_loop.READ:
                self._handle_read()
            if self._closed:
                return
            if events & self.io_loop.WRITE:
                self._handle_write()
            if self._closed:
                return
            if events & self.io_loop.ERROR:
                self.error = self.get_fd_error()
                self.io_loop.add_callback(self.close)
                return
        except Exception:
            self.close(exc_info=True)
            raise

    def _handle_connect(self):
        super(IOStream, self)._handle_connect()

        if not self.closed():
            self._state = self.io_loop.ERROR | self.io_loop.READ
            if self._write_buffer:
                self._state = self._state | self.io_loop.WRITE
            self.io_loop.update_handler(self.fileno(), self._state)

    def _handle_read(self):
        chunk = True

        while True:
            try:
                chunk = self.socket.recv(self.read_chunk_size)
                if not chunk:
                    break
                if self._read_buffer_size:
                    self._read_buffer += chunk
                else:
                    self._read_buffer = bytearray(chunk)
                self._read_buffer_size += len(chunk)
            except (socket.error, IOError, OSError) as e:
                en = e.errno if hasattr(e, 'errno') else e.args[0]
                if en in _ERRNO_WOULDBLOCK:
                    break

                if en == errno.EINTR:
                    continue

                self.close(exc_info=True)
                return

        if self._read_future is not None and self._read_buffer_size >= self._read_bytes:
            future, self._read_future = self._read_future, None
            self._read_buffer, data = bytearray(), self._read_buffer
            self._read_buffer_size = 0
            self._read_bytes = 0
            future.set_result(data)

        if not chunk:
            self.close()
            return

    def read(self, num_bytes):
        assert self._read_future is None, "Already reading"
        if self._closed:
            raise StreamClosedError(real_error=self.error)

        future = self._read_future = Future()
        self._read_bytes = num_bytes
        self._read_partial = False
        if self._read_buffer_size >= self._read_bytes:
            future, self._read_future = self._read_future, None
            self._read_buffer, data = bytearray(), self._read_buffer
            self._read_buffer_size = 0
            self._read_bytes = 0
            future.set_result(data)
        return future

    read_bytes = read

    def _handle_write(self):
        try:
            num_bytes = self.socket.send(memoryview(self._write_buffer)[self._write_buffer_pos: self._write_buffer_pos + self._write_buffer_size])
            self._write_buffer_pos += num_bytes
            self._write_buffer_size -= num_bytes
        except (socket.error, IOError, OSError) as e:
            en = e.errno if hasattr(e, 'errno') else e.args[0]
            if en not in _ERRNO_WOULDBLOCK:
                self.close(exc_info=True)
                return

        if not self._write_buffer_size:
            if self._write_buffer_pos > 0:
                self._write_buffer = bytearray()
                self._write_buffer_pos = 0

            if self._state & self.io_loop.WRITE:
                self._state = self._state & ~self.io_loop.WRITE
                self.io_loop.update_handler(self.fileno(), self._state)

    def write(self, data):
        assert isinstance(data, (bytes, bytearray))
        if self._closed:
            raise StreamClosedError(real_error=self.error)

        if data:
            if self._write_buffer_size:
                self._write_buffer += data
            else:
                self._write_buffer = bytearray(data)
            self._write_buffer_size += len(data)

        if not self._connecting:
            self._handle_write()
            if self._write_buffer_size:
                if not self._state & self.io_loop.WRITE:
                    self._state = self._state | self.io_loop.WRITE
                    self.io_loop.update_handler(self.fileno(), self._state)