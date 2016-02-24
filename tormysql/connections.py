# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from __future__ import absolute_import, division, print_function, with_statement

import greenlet
import socket
import sys
import struct
import traceback
from pymysql import err
from pymysql.charset import charset_by_name
from pymysql.constants import COMMAND, CLIENT
from pymysql.connections import Connection as _Connection, lenenc_int, text_type
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
    def __init__(self, *args, **kwargs):
        self._close_callback = None
        self._rbuffer = StringIO(b'')
        self._rbuffer_size = 0
        self._loop = None
        self.socket = None
        super(Connection, self).__init__(*args, **kwargs)

        self._loop_connect_timeout = None

    def set_close_callback(self, callback):
        self._close_callback = callback
        
    def stream_close_callback(self):
        if self._close_callback and callable(self._close_callback):
            cb = self._close_callback
            self._close_callback = None
            cb()
                
        if self.socket:
            sock = self.socket
            self.socket = None
            self._rfile = None
            sock.set_close_callback(None)

    def close(self):
        if self._close_callback and callable(self._close_callback):
            cb = self._close_callback
            self._close_callback = None
            cb()

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

    @property
    def open(self):
        return self.socket is not None and not self.socket.closed()

    def __del__(self):
        if self.socket:
            self.close()

    def connect(self):
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
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock = IOStream(sock)
            sock.set_close_callback(self.stream_close_callback)

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "Execut must be running in child greenlet"

            self._loop_connect_timeout = None
            if self.connect_timeout:
                def timeout():
                    self._loop_connect_timeout = None
                    if not self.socket:
                        sock.close((None, IOError("connect timeout"), None))
                self._loop_connect_timeout = self._loop.call_later(self.connect_timeout, timeout)

            def connected(future):
                if self._loop_connect_timeout:
                    self._loop.remove_timeout(self._loop_connect_timeout)
                    self._loop_connect_timeout = None

                if future._exc_info is not None:
                    child_gr.throw(future.exception())
                else:
                    self.socket = sock
                    child_gr.switch()

            future = sock.connect(address)
            self._loop.add_future(future, connected)
            main.switch()

            self._rfile = self.socket
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
            if self.socket:
                self._rfile = None
                self.socket.close()
                self.socket = None
            exc = err.OperationalError(
                2003, "Can't connect to MySQL server on %s (%r)" % (self.unix_socket or ("%s:%s" % (self.host, self.port)), e))
            # Keep original exception and traceback to investigate error.
            exc.original_exception = e
            exc.traceback = traceback.format_exc()
            raise exc

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
            if self._rfile._state and (self._rfile._state & self._rfile.io_loop.READ == 0):
                self._rfile._state |= self._rfile.io_loop.READ
                self._rfile.io_loop.update_handler(self._rfile.fileno(), self._rfile._state)
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
        if int(self.server_version.split('.', 1)[0]) >= 5:
            self.client_flag |= CLIENT.MULTI_RESULTS

        if self.user is None:
            raise ValueError("Did not specify a username")

        charset_id = charset_by_name(self.charset).id
        if isinstance(self.user, text_type):
            self.user = self.user.encode(self.encoding)

        data_init = struct.pack('<iIB23s', self.client_flag, 1, charset_id, b'')

        if self.ssl and self.server_capabilities & CLIENT.SSL:
            self.write_packet(data_init)

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "Execut must be running in child greenlet"

            def finish(future):
                if future._exc_info is not None:
                    child_gr.throw(future.exception())
                else:
                    child_gr.switch(future.result())

            future = self.socket.start_tls(False, self.ctx, server_hostname=self.host)
            self._loop.add_future(future, finish)
            self._rfile = self.socket = main.switch()

        data = data_init + self.user + b'\0'

        authresp = b''
        if self._auth_plugin_name in ('', 'mysql_native_password'):
            authresp = _scramble(self.password.encode('latin1'), self.salt)

        if self.server_capabilities & CLIENT.PLUGIN_AUTH_LENENC_CLIENT_DATA:
            data += lenenc_int(len(authresp)) + authresp
        elif self.server_capabilities & CLIENT.SECURE_CONNECTION:
            data += struct.pack('B', len(authresp)) + authresp
        else:  # pragma: no cover - not testing against servers without secure auth (>=5.0)
            data += authresp + b'\0'

        if self.db and self.server_capabilities & CLIENT.CONNECT_WITH_DB:
            if isinstance(self.db, text_type):
                self.db = self.db.encode(self.encoding)
            data += self.db + b'\0'

        if self.server_capabilities & CLIENT.PLUGIN_AUTH:
            name = self._auth_plugin_name
            if isinstance(name, text_type):
                name = name.encode('ascii')
            data += name + b'\0'

        self.write_packet(data)
        auth_packet = self._read_packet()

        # if authentication method isn't accepted the first byte
        # will have the octet 254
        if auth_packet.is_auth_switch_request():
            # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
            auth_packet.read_uint8() # 0xfe packet identifier
            plugin_name = auth_packet.read_string()
            if self.server_capabilities & CLIENT.PLUGIN_AUTH and plugin_name is not None:
                auth_packet = self._process_auth(plugin_name, auth_packet)
            else:
                # send legacy handshake
                data = _scramble_323(self.password.encode('latin1'), self.salt) + b'\0'
                self.write_packet(data)
                auth_packet = self._read_packet()
