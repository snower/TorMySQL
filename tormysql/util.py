# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower
import sys
import greenlet
from tornado.ioloop import IOLoop
from tornado.concurrent import Future


def async_call_method(fun, *args, **kwargs):
    future = Future()
    io_loop = IOLoop.current()

    def finish():
        try:
            result = fun(*args, **kwargs)
            io_loop.add_callback(future.set_result, result)
        except:
            io_loop.add_callback(future.set_exc_info, sys.exc_info())

    child_gr = greenlet.greenlet(finish)
    child_gr.switch()

    return future
