# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

import greenlet
from pymysql.connections import *
from pymysql.connections import _scramble
from tornado.iostream import IOStream
from tornado.ioloop import IOLoop

class Connection(Connection):
    def _connect(self):
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
            self.socket = IOStream(sock)

            child_gr = greenlet.getcurrent()
            main = child_gr.parent
            assert main is not None, "Execut must be running in child greenlet"
            self.socket.connect(address, lambda :child_gr.switch())
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
            self._rfile = None
            self.socket.close()
            self.socket = None
            raise OperationalError(
                2003, "Can't connect to MySQL server on %r (%s)" % (self.host, e))

    def _read_bytes(self, num_bytes):
        child_gr = greenlet.getcurrent()
        main = child_gr.parent
        assert main is not None, "Execut must be running in child greenlet"
        self._rfile.read_bytes(num_bytes, lambda data:child_gr.switch(data))
        return main.switch()

    def _write_bytes(self, data):
        self.socket.write(data)

    def _request_authentication(self):
        self.client_flag |= CAPABILITIES
        if self.server_version.startswith('5'):
            self.client_flag |= MULTI_RESULTS

        if self.user is None:
            raise ValueError("Did not specify a username")

        charset_id = charset_by_name(self.charset).id
        if isinstance(self.user, text_type):
            self.user = self.user.encode(self.encoding)

        data_init = struct.pack('<i', self.client_flag) + struct.pack("<I", 1) + \
                     int2byte(charset_id) + int2byte(0)*23

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
                except Exception,e:
                    child_gr.throw(e)

            future = self.socket.start_tls(None, {
                "keyfile":self.key,
                "certfile":self.cert,
                "ssl_version":ssl.PROTOCOL_TLSv1,
                "cert_reqs":ssl.CERT_REQUIRED,
                "ca_certs":self.ca,
            })
            IOLoop.current().add_future(future, finish)
            self.socket = main.switch()
            self._rfile = _makefile(self.socket, 'rb')

        data = data_init + self.user + b'\0' + \
            _scramble(self.password.encode('latin1'), self.salt)

        if self.db:
            if isinstance(self.db, text_type):
                self.db = self.db.encode(self.encoding)
            data += self.db + int2byte(0)

        data = pack_int24(len(data)) + int2byte(next_packet) + data
        next_packet += 2

        if DEBUG: dump_packet(data)

        self._write_bytes(data)

        auth_packet = MysqlPacket(self)
        auth_packet.check_error()
        if DEBUG: auth_packet.dump()

        # if old_passwords is enabled the packet will be 1 byte long and
        # have the octet 254

        if auth_packet.is_eof_packet():
            # send legacy handshake
            data = _scramble_323(self.password.encode('latin1'), self.salt) + b'\0'
            data = pack_int24(len(data)) + int2byte(next_packet) + data

            self._write_bytes(data)
            auth_packet = MysqlPacket(self)
            auth_packet.check_error()
            if DEBUG: auth_packet.dump()