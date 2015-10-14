# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower
import sys
import greenlet
from tornado.ioloop import IOLoop
from tornado.concurrent import Future

def async_call_method(fun, *args, **kwargs):
    future = Future()

    def finish():
        try:
            result = fun(*args, **kwargs)
            if future._callbacks:
                IOLoop.current().add_callback(future.set_result, result)
            else:
                future.set_result(result)
        except:
            if future._callbacks:
                IOLoop.current().add_callback(future.set_exc_info, sys.exc_info())
            else:
                future.set_exc_info(sys.exc_info())

    child_gr = greenlet.greenlet(finish)
    child_gr.switch()

    return future
