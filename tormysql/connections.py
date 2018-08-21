# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from __future__ import absolute_import, division, print_function, with_statement

import greenlet
import sys
import struct
import traceback
from pymysql import err
from pymysql.constants import CR
from pymysql.connections import Connection as _Connection
from . import platform

if sys.version_info[0] >= 3:
    import io
    StringIO = io.BytesIO
else:
    import cStringIO
    StringIO = cStringIO.StringIO

class SSLCtx(object):
    _ctx = None
    _connection = None

    def __init__(self, connection, ctx):
        self._ctx = ctx
        self._connection = connection

    def __getattr__(self, item):
        return getattr(self._ctx, item)

    def __setattr__(self, key, value):
        if not self or not self._ctx or not self._connection:
            return super(SSLCtx, self).__setattr__(key, value)
        return setattr(self._ctx, key, value)

    def __getitem__(self, item):
        return self._ctx[item]

    def wrap_socket(self, sock, server_side=False,
                    do_handshake_on_connect=True,
                    suppress_ragged_eofs=True,
                    server_hostname=None, session=None):

        child_gr = greenlet.getcurrent()
        main = child_gr.parent
        assert main is not None, "Execut must be running in child greenlet"

        def finish(future):
            if (hasattr(future, "_exc_info") and future._exc_info is not None) \
                    or (hasattr(future, "_exception") and future._exception is not None):
                child_gr.throw(future.exception())
            else:
                child_gr.switch(future.result())

        future = sock.start_tls(False, self._ctx, server_hostname=server_hostname, connect_timeout=self._connection.connect_timeout)
        future.add_done_callback(finish)
        return main.switch()

class Connection(_Connection):
    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)

        self._close_callback = None
        self._rbuffer = StringIO(b'')
        self._rbuffer_size = 0
        self._loop = None

    def set_close_callback(self, callback):
        self._close_callback = callback
        
    def stream_close_callback(self):
        if self._close_callback and callable(self._close_callback):
            close_callback, self._close_callback = self._close_callback, None
            close_callback()
                
        if self._sock:
            self._sock.set_close_callback(None)
            self._sock = None
            self._rfile = None
            self.ctx = None

    @property
    def open(self):
        return self._sock and not self._sock.closed()

    def _force_close(self):
        if self._sock:
            try:
                sock = self._sock
                self._sock = None
                self._rfile = None
                sock.close()
            except:
                pass
        self.ctx = None

    __del__ = _force_close

    def _create_ssl_ctx(self, sslp):
        ctx = super(Connection, self)._create_ssl_ctx(sslp)
        return SSLCtx(self, ctx)

    def connect(self):
        self._closed = False
        self._loop = platform.current_ioloop()
        try:
            if self.unix_socket:
                self.host_info = "Localhost via UNIX socket"
                address = self.unix_socket
                self._secure = True
            else:
                self.host_info = "socket %s:%d" % (self.host, self.port)
                address = (self.host, self.port)
            sock = platform.IOStream(address, self.bind_address)
            sock.set_close_callback(self.stream_close_callback)

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "Execut must be running in child greenlet"

            def connected(future):
                if (hasattr(future, "_exc_info") and future._exc_info is not None) \
                        or (hasattr(future, "_exception") and future._exception is not None):
                    child_gr.throw(future.exception())
                else:
                    self._sock = sock
                    child_gr.switch()

            future = sock.connect(address, self.connect_timeout)
            future.add_done_callback(connected)
            main.switch()

            self._rfile = self._sock
            self._next_seq_id = 0

            self._get_server_information()
            self._request_authentication()

            if self.sql_mode is not None:
                c = self.cursor()
                c.execute("SET sql_mode=%s", (self.sql_mode,))

            if self.init_command is not None:
                c = self.cursor()
                c.execute(self.init_command)
                self.commit()

            if self.autocommit_mode is not None:
                self.autocommit(self.autocommit_mode)
        except Exception as e:
            if self._sock:
                self._rfile = None
                self._sock.close()
                self._sock = None
            exc = err.OperationalError(
                2003, "Can't connect to MySQL server on %s (%r)" % (
                self.unix_socket or ("%s:%s" % (self.host, self.port)), e))
            # Keep original exception and traceback to investigate error.
            exc.original_exception = e
            exc.traceback = traceback.format_exc()
            raise exc

    def _read_bytes(self, num_bytes):
        if num_bytes <= self._rbuffer_size:
            self._rbuffer_size -= num_bytes
            return self._rbuffer.read(num_bytes)

        if self._rbuffer_size > 0:
            self._sock._read_buffer = self._rbuffer.read() + self._sock._read_buffer
            self._sock._read_buffer_size += self._rbuffer_size
            self._rbuffer_size = 0

        if num_bytes <= self._sock._read_buffer_size:
            data, data_len = self._sock._read_buffer, self._sock._read_buffer_size
            self._sock._read_buffer = bytearray()
            self._sock._read_buffer_size = 0

            if data_len == num_bytes:
                return bytes(data)

            self._rbuffer_size = data_len - num_bytes
            self._rbuffer = StringIO(data)
            return self._rbuffer.read(num_bytes)

        child_gr = greenlet.getcurrent()
        main = child_gr.parent
        assert main is not None, "Execut must be running in child greenlet"

        def read_callback(future):
            try:
                data = future.result()
                if len(data) == num_bytes:
                    return child_gr.switch(bytes(data))

                self._rbuffer_size = len(data) - num_bytes
                self._rbuffer = StringIO(data)
                return child_gr.switch(self._rbuffer.read(num_bytes))
            except Exception as e:
                self._force_close()
                return child_gr.throw(err.OperationalError(
                    CR.CR_SERVER_LOST,
                    "Lost connection to MySQL server during query (%s)" % (e,)))
        try:
            future = self._sock.read_bytes(num_bytes)
            future.add_done_callback(read_callback)
        except (AttributeError, IOError) as e:
            self._force_close()
            raise err.OperationalError(
                CR.CR_SERVER_LOST,
                "Lost connection to MySQL server during query (%s)" % (e,))
        return main.switch()

    def _write_bytes(self, data):
        try:
            self._sock.write(data)
        except (AttributeError, IOError) as e:
            self._force_close()
            raise err.OperationalError(
                CR.CR_SERVER_GONE_ERROR,
                "MySQL server has gone away (%r)" % (e,))

    def _request_authentication(self):
        super(Connection, self)._request_authentication()

        self._rfile = self._sock

    def __str__(self):
        return "%s %s" % (super(Connection, self).__str__(),
                          {"host": self.host or self.unix_socket, "user": self.user, "database": self.db,
                           "port": self.port})