TorMySQL
========

.. image:: https://travis-ci.org/mosquito/TorMySQL.svg
    :target: https://travis-ci.org/mosquito/TorMySQL

Tornado asynchronous MySQL Driver

About
=====

TorMySQL presents a Tornado Future-based API and greenlet for
non-blocking access to MySQL.

Installation
============

::

    pip install TorMySQL

Examples
========

::

    from tornado.ioloop import IOLoop
    from tornado import gen
    import tormysql

    pool = tormysql.ConnectionPool(
        max_connections = 20, #max open connections
        idle_seconds = 7200, #conntion idle timeout time, 0 is not timeout
        host = "127.0.0.1",
        user = "root",
        passwd = "TEST",
        db = "test",
        charset = "utf8"
    )

    @gen.coroutine
    def test():
        conn = yield pool.Connection()
        cursor = conn.cursor()
        yield cursor.execute("SELECT * FROM test")
        datas = cursor.fetchall()
        yield cursor.close()
        conn.close()

        print datas
        
        yield pool.close()

    ioloop = IOLoop.instance()
    ioloop.add_callback(test)
    ioloop.start()
