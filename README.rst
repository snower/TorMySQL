TorMySQL
========

.. image:: https://travis-ci.org/snower/TorMySQL.svg
    :target: https://travis-ci.org/snower/TorMySQL

Tornado asynchronous MySQL Driver.
PyPI page: https://pypi.python.org/pypi/tormysql

About
=====

tormysql - presents a Tornado Future-based API and greenlet for non-blocking access to MySQL.

Installation
============

::

    pip install TorMySQL

Examples
========

example pool
~~~~~~~

::

    from tornado.ioloop import IOLoop
    from tornado import gen
    import tormysql

    pool = tormysql.ConnectionPool(
        max_connections = 20, #max open connections
        idle_seconds = 7200, #conntion idle timeout time, 0 is not timeout
        wait_connection_timeout = 3, #wait connection timeout
        host = "127.0.0.1",
        user = "root",
        passwd = "TEST",
        db = "test",
        charset = "utf8"
    )

    @gen.coroutine
    def test():
        with (yield pool.Connection()) as conn:
            try:
                with conn.cursor() as cursor:
                    yield cursor.execute("INSERT INTO test(id) VALUES(1)")
            except:
                yield conn.rollback()
            else:
                yield conn.commit()

            with conn.cursor() as cursor:
                yield cursor.execute("SELECT * FROM test")
                datas = cursor.fetchall()

        print datas
        
        yield pool.close()

    ioloop = IOLoop.instance()
    ioloop.run_sync(test)

example helpers
~~~~~~~

::

    from tornado.ioloop import IOLoop
    from tornado import gen
    import tormysql

    pool = tormysql.helpers.ConnectionPool(
        max_connections = 20, #max open connections
        idle_seconds = 7200, #conntion idle timeout time, 0 is not timeout
        wait_connection_timeout = 3, #wait connection timeout
        host = "127.0.0.1",
        user = "root",
        passwd = "TEST",
        db = "test",
        charset = "utf8"
    )

    @gen.coroutine
    def test():
        tx = yield pool.begin()
        try:
            yield tx.execute("INSERT INTO test(id) VALUES(1)")
        except:
            yield tx.rollback()
        else:
            yield tx.commit()

        cursor = yield pool.execute("SELECT * FROM test")
        datas = cursor.fetchall()

        print datas

        yield pool.close()

    ioloop = IOLoop.instance()
    ioloop.run_sync(test)

License
========

TorMySQL uses the MIT license, see LICENSE file for the details.