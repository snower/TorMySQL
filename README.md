TorMySQL
========

Tornado asynchronous MySQL Driver

About
=====

TorMySQL presents a Tornado Future-based API and greenlet for non-blocking access
to MySQL.

Installation
============

```
git clone https://github.com/snower/TorMySQL.git
python setup.py install
```

Examples
========

```
from tornado.ioloop import IOLoop
from tornado import gen
import tormysql

pool = tormysql.ConnectionPool(
    max_connections = 20,
    host = "127.0.0.1",
    user = "root",
    passwd = "TEST",
    db = "test",
    charset = "utf8"
)

@gen.coroutine
def connect():
    conn = yield pool.Connection()
    cursor = yield conn.cursor()
    yield cursor.execute("SELECT * FROM test")
    datas = yield cursor.fetchall()
    yield cursor.close()
    yield conn.close()

    print datas

def start():
    connect()

ioloop = IOLoop.instance()
ioloop.add_callback(start)
ioloop.start()
```
