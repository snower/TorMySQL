# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

import sys
import greenlet
from tornado.ioloop import IOLoop
from tornado.concurrent import TracebackFuture

def async_call_method(fun, *args, **kwargs):
    future = TracebackFuture()
    def finish():
        try:
            result = fun(*args, **kwargs)
            IOLoop.current().add_callback(lambda :future.set_result(result))
        except:
            exc_info = sys.exc_info()
            IOLoop.current().add_callback(lambda :future.set_exc_info(exc_info))
    child_gr = greenlet.greenlet(finish)
    child_gr.switch()
    return future