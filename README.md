# TorMySQL

[![Build Status](https://travis-ci.org/snower/TorMySQL.svg?branch=master)](https://travis-ci.org/snower/TorMySQL)

The highest performance asynchronous MySQL driver.

PyPI page: https://pypi.python.org/pypi/tormysql

# About

Presents a Future-based API and greenlet for non-blocking access to MySQL.

Support both [tornado](https://github.com/tornadoweb/tornado) and [asyncio](https://docs.python.org/3/library/asyncio.html).

# Installation

```
pip install TorMySQL
```

# Used Tornado

## example pool

```
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
```

## example helpers

```
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
```

# Used asyncio alone

## example pool

```
from asyncio import events
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

async def test():
   async with await pool.Connection() as conn:
       try:
           async with conn.cursor() as cursor:
               await cursor.execute("INSERT INTO test(id) VALUES(1)")
       except:
           await conn.rollback()
       else:
           await conn.commit()

       async with conn.cursor() as cursor:
           await cursor.execute("SELECT * FROM test")
           datas = cursor.fetchall()

   print(datas)

   await pool.close()

ioloop = events.get_event_loop()
ioloop.run_until_complete(test)
```

## example helpers

```
from asyncio import events
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

async def test():
   async with await pool.begin() as tx:
       await tx.execute("INSERT INTO test(id) VALUES(1)")

   cursor = await pool.execute("SELECT * FROM test")
   datas = cursor.fetchall()

   print(datas)

   await pool.close()

ioloop = events.get_event_loop()
ioloop.run_until_complete(test)
```

# Resources

You can read [PyMySQL Documentation](http://pymysql.readthedocs.io/) online for more information.

# License

TorMySQL uses the MIT license, see LICENSE file for the details.