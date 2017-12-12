# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from __future__ import absolute_import, division, print_function, with_statement

import greenlet
import sys
import struct
import traceback
from pymysql import err
from pymysql.charset import charset_by_name
from pymysql.constants import COMMAND, CLIENT, CR
from pymysql.connections import Connection as _Connection, lenenc_int, text_type
from pymysql.connections import _scramble, _scramble_323
from . import platform

if sys.version_info[0] >= 3:
    import io
    StringIO = io.BytesIO
else:
    import cStringIO
    StringIO = cStringIO.StringIO

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

    def close(self):
        if self._closed:
            raise err.Error("Already closed")
        self._closed = True
        if self._sock is None:
            return

        send_data = struct.pack('<iB', 1, COMMAND.COM_QUIT)
        try:
            self._write_bytes(send_data)
        except Exception:
            pass
        finally:
            sock = self._sock
            self._sock = None
            self._rfile = None
            sock.close()

    @property
    def open(self):
        return self._sock and not self._sock.closed()

    def _force_close(self):
        if self._sock:
            try:
                self._sock.close()
            except:
                pass
        self._sock = None
        self._rfile = None

    __del__ = _force_close

    def connect(self):
        self._closed = False
        self._loop = platform.current_ioloop()
        try:
            if self.unix_socket and self.host in ('localhost', '127.0.0.1'):
                self.host_info = "Localhost via UNIX socket"
                address = self.unix_socket
            else:
                self.host_info = "socket %s:%d" % (self.host, self.port)
                address = (self.host, self.port)
            sock = platform.IOStream(address, self.bind_address)
            sock.set_close_callback(self.stream_close_callback)

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "Execut must be running in child greenlet"

            def connected(future):
                if (hasattr(future, "_exc_info") and future._exc_info is not None) or (hasattr(future, "_exception") and future._exception is not None):
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
            self._rfile._read_buffer = self._rbuffer.read() + self._rfile._read_buffer
            self._rfile._read_buffer_size += self._rbuffer_size
            self._rbuffer_size = 0

        if num_bytes <= self._rfile._read_buffer_size:
            data, data_len = self._rfile._read_buffer, self._rfile._read_buffer_size
            self._rfile._read_buffer = bytearray()
            self._rfile._read_buffer_size = 0

            if data_len == num_bytes:
                return data

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
                    return child_gr.switch(data)

                self._rbuffer_size = len(data) - num_bytes
                self._rbuffer = StringIO(data)
                return child_gr.switch(self._rbuffer.read(num_bytes))
            except Exception as e:
                self._force_close()
                return child_gr.throw(err.OperationalError(
                    CR.CR_SERVER_LOST,
                    "Lost connection to MySQL server during query (%s)" % (e,)))
        try:
            future = self._rfile.read_bytes(num_bytes)
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
                if (hasattr(future, "_exc_info") and future._exc_info is not None) \
                        or (hasattr(future, "_exception") and future._exception is not None):
                    child_gr.throw(future.exception())
                else:
                    child_gr.switch(future.result())

            future = self._sock.start_tls(False, self.ctx, server_hostname=self.host)
            future.add_done_callback(finish)
            self._rfile = self._sock = main.switch()

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
            auth_packet.read_uint8()  # 0xfe packet identifier
            plugin_name = auth_packet.read_string()
            if self.server_capabilities & CLIENT.PLUGIN_AUTH and plugin_name is not None:
                auth_packet = self._process_auth(plugin_name, auth_packet)
            else:
                # send legacy handshake
                data = _scramble_323(self.password.encode('latin1'), self.salt) + b'\0'
                self.write_packet(data)
                auth_packet = self._read_packet()

    def __str__(self):
        return "%s %s" % (super(Connection, self).__str__(),
                          {"host": self.host or self.unix_socket, "user": self.user, "database": self.db,
                           "port": self.port})