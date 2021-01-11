# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

import sys
import greenlet
from . import platform

if sys.version_info[0] >= 3:
    py3 = True
    text_type = str
else:
    py3 = False
    text_type = unicode

def async_call_method(fun, *args, **kwargs):
    future = platform.Future()
    ioloop = platform.current_ioloop()

    def finish():
        try:
            result = fun(*args, **kwargs)
            if future._callbacks:
                ioloop.call_soon(future.set_result, result)
            else:
                future.set_result(result)
        except Exception as e:
            if future._callbacks:
                ioloop.call_soon(future.set_exception, e)
            else:
                future.set_exception(e)

    child_gr = greenlet.greenlet(finish)
    child_gr.switch()

    return future
