# -*- coding: utf-8 -*-
# 17/12/8
# create by: snower

from __future__ import absolute_import, division, print_function

try:
    import asyncio
    from .asyncio import StreamClosedError
except ImportError:
    asyncio = None
    from tornado.iostream import StreamClosedError

class IOLoop(object):
    _instance = None

    def __init__(self):
        self.ioloop = None
        self.call_soon = None
        self.call_at = None
        self.call_later = None
        self.cancel_timeout = None

    def __getattr__(self, name):
        return getattr(self.ioloop, name)

IOLoop._instance = IOLoop()
Future, coroutine, IOStream = None, None, None
current_ioloop = None
is_reset = False

def use_tornado(reset = True):
    global Future, coroutine, IOStream, current_ioloop, is_reset
    if not reset and is_reset:
        return
    is_reset = reset

    from .tornado import Future, coroutine, IOStream

    def tornado_current_ioloop():
        global current_ioloop
        if IOLoop._instance.ioloop is None:
            from .tornado import current_ioloop as _current_ioloop
            IOLoop._instance.ioloop = _current_ioloop()
            IOLoop._instance.call_soon = IOLoop._instance.ioloop.add_callback
            IOLoop._instance.call_at = IOLoop._instance.ioloop.call_at
            IOLoop._instance.call_later = IOLoop._instance.ioloop.call_later
            IOLoop._instance.cancel_timeout = IOLoop._instance.ioloop.remove_timeout
            current_ioloop = lambda : IOLoop._instance
        return IOLoop._instance

    current_ioloop = tornado_current_ioloop
    return current_ioloop

def use_asyncio(reset = True):
    global Future, coroutine, IOStream, current_ioloop, is_reset
    if not reset and is_reset:
        return
    is_reset = reset

    from .asyncio import Future, coroutine, IOStream

    def asyncio_current_ioloop():
        global current_ioloop

        if IOLoop._instance.ioloop is None:
            try:
                from tornado.ioloop import IOLoop as TornadoIOLoop
                from tornado.platform.asyncio import AsyncIOMainLoop
                tornado_ioloop = TornadoIOLoop.current(False)
                if isinstance(tornado_ioloop, TornadoIOLoop) and not isinstance(tornado_ioloop, AsyncIOMainLoop):
                    return use_tornado(False)()
            except: pass

            from .asyncio import current_ioloop as _current_ioloop
            IOLoop._instance.ioloop = _current_ioloop()
            IOLoop._instance.call_soon = IOLoop._instance.ioloop.call_soon
            IOLoop._instance.call_at = IOLoop._instance.ioloop.call_at
            IOLoop._instance.call_later = IOLoop._instance.ioloop.call_later

            def cancel_timeout(timeout):
                timeout.cancel()
            IOLoop._instance.cancel_timeout = cancel_timeout
        current_ioloop = lambda: IOLoop._instance
        return IOLoop._instance

    current_ioloop = asyncio_current_ioloop
    return current_ioloop

if asyncio is None:
    use_tornado(False)
else:
    use_asyncio(False)