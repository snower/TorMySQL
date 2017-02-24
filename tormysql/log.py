# -*- coding: utf-8 -*-
# 17/2/17
# create by: snower

import logging

_log = logging

def get_log():
    return _log

def set_log(log):
    global _log
    _log = log