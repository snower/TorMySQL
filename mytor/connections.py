# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from __future__ import absolute_import, division, print_function, with_statement

import greenlet
import socket
import ssl
import sys
import struct
from pymysql import err, text_type
from pymysql.charset import charset_by_name
from pymysql.util import int2byte
from pymysql.constants import COMMAND, CLIENT
from pymysql.connections import Connection as _Connection, pack_int24, dump_packet, DEBUG
from pymysql.connections import _scramble, _scramble_323
from tornado.iostream import IOStream, StreamClosedError
from tornado.ioloop import IOLoop


if sys.version_info[0] >=3:
    import io
    StringIO = io.BytesIO
else:
    import cStringIO
    StringIO = cStringIO.StringIO


class Connection(_Connection):
    _socket = None
    __slots__ = ['socket', '_loop', '_rfile', '_rbuffer', '_rbuffer_size', '_close_callback']

    def __getattr__(self, item):
        if item is 'socket':
            return self._socket
        return getattr(self, item)

    def __init__(self, *args, **kwargs):
        self._close_callback = None
        self._rbuffer = StringIO(b'')
        self._rbuffer_size = 0
        self._loop = None
        super(Connection, self).__init__(*args, **kwargs)

    def set_close_callback(self, callback):
        self._close_callback = callback

    def close(self):
        if self._close_callback and callable(self._close_callback):
            self._close_callback()

        if self.socket is None:
            raise err.Error("Already closed")

        send_data = struct.pack('<iB', 1, COMMAND.COM_QUIT)
        try:
            self._write_bytes(send_data)
        except Exception:
            pass
        finally:
            sock = self.socket
            self.socket = None
            self._rfile = None
            sock.set_close_callback(None)
            sock.close()

    def __del__(self):
        if self.socket:
            self.close()

    def _connect(self):
        self._loop = IOLoop.current()
        try:
            if self.unix_socket and self.host in ('localhost', '127.0.0.1'):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.host_info = "Localhost via UNIX socket"
                address = self.unix_socket
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
                self.host_info = "socket %s:%d" % (self.host, self.port)
                address = (self.host, self.port)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if self.no_delay:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            child_gr = greenlet.getcurrent()
            sock = IOStream(sock)

            def on_close(reason=None):
                if self.socket:
                    return self.close()
                else:
                    return child_gr.throw(IOError(reason))

            sock.set_close_callback(on_close)

            main = child_gr.parent
            assert main is not None, "Execut must be running in child greenlet"

            if self.connect_timeout:
                def timeout():
                    if not self.socket:
                        sock.close()
                        child_gr.throw(IOError("connection timeout"))

                IOLoop.current().call_later(self.connect_timeout, timeout)

            def connected():
                self.socket = sock
                child_gr.switch()

            sock.connect(address, connected)
            main.switch()

            self._rfile = self.socket
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
            if self.socket:
                self._rfile = None
                self.socket.close()
                self.socket = None
            raise err.OperationalError(
                2003, "Can't connect to MySQL server on %r (%s)" % (self.host, e))

    def _read_bytes(self, num_bytes):
        if num_bytes <= self._rbuffer_size:
            self._rbuffer_size -= num_bytes
            return self._rbuffer.read(num_bytes)

        if num_bytes <= self._rfile._read_buffer_size + self._rbuffer_size:
            last_buf = b''
            if self._rbuffer_size > 0:
                last_buf += self._rbuffer.read()
            self._rbuffer_size = self._rfile._read_buffer_size + self._rbuffer_size - num_bytes
            self._rbuffer = StringIO(last_buf + b''.join(self._rfile._read_buffer))
            self._rfile._read_buffer.clear()
            self._rfile._read_buffer_size = 0
            return self._rbuffer.read(num_bytes)

        child_gr = greenlet.getcurrent()
        main = child_gr.parent
        assert main is not None, "Execut must be running in child greenlet"

        def read_callback(future):
            if future._exc_info is not None:
                return child_gr.throw(err.OperationalError(2006, "MySQL server has gone away (%r)" % (future.exception(),)))

            data = future.result()
            last_buf = b''
            if self._rbuffer_size > 0:
                last_buf += self._rbuffer.read()
            self._rbuffer_size = 0
            return child_gr.switch(last_buf + data)
        try:
            future = self._rfile.read_bytes(num_bytes - self._rbuffer_size)
            self._loop.add_future(future, read_callback)
        except (AttributeError, StreamClosedError) as e:
            raise err.OperationalError(2006, "MySQL server has gone away (%r)" % (e,))
        return main.switch()

    def _write_bytes(self, data):
        try:
            self.socket.write(data)
        except (AttributeError, StreamClosedError) as e:
            raise err.OperationalError(2006, "MySQL server has gone away (%r)" % (e,))

    def _request_authentication(self):
        self.client_flag |= CLIENT.CAPABILITIES
        if self.server_version.startswith('5'):
            self.client_flag |= CLIENT.MULTI_RESULTS

        if self.user is None:
            raise ValueError("Did not specify a username")

        charset_id = charset_by_name(self.charset).id
        if isinstance(self.user, text_type):
            self.user = self.user.encode(self.encoding)

        data_init = struct.pack('<iIB23s', self.client_flag, 1, charset_id, b'')

        next_packet = 1

        if self.ssl:
            data = pack_int24(len(data_init)) + int2byte(next_packet) + data_init
            next_packet += 1

            self._write_bytes(data)

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "Execut must be running in child greenlet"

            def finish(future):
                try:
                    stream = future.result()
                    child_gr.switch(stream)
                except Exception as e:
                    child_gr.throw(e)

            future = self.socket.start_tls(None, {
                "keyfile": self.key,
                "certfile": self.cert,
                "ssl_version": ssl.PROTOCOL_TLSv1,
                "cert_reqs": ssl.CERT_REQUIRED,
                "ca_certs": self.ca,
            })
            IOLoop.current().add_future(future, finish)
            self.socket = main.switch()
            self._rfile = self.socket

        data = data_init + self.user + b'\0' + \
            _scramble(self.password.encode('latin1'), self.salt)

        if self.db:
            if isinstance(self.db, text_type):
                self.db = self.db.encode(self.encoding)
            data += self.db + int2byte(0)

        data = pack_int24(len(data)) + int2byte(next_packet) + data
        next_packet += 2

        if DEBUG:
            dump_packet(data)

        self._write_bytes(data)

        auth_packet = self._read_packet()

        # if old_passwords is enabled the packet will be 1 byte long and
        # have the octet 254

        if auth_packet.is_eof_packet():
            # send legacy handshake
            data = _scramble_323(self.password.encode('latin1'), self.salt) + b'\0'
            data = pack_int24(len(data)) + int2byte(next_packet) + data
            self._write_bytes(data)
            auth_packet = self._read_packet()
