#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from setuptools import setup


setup(
    name='TorMySQL',
    version='0.2.0',
    packages=['tormysql'],
    install_requires=[
        'tornado>=4.1',
        'PyMySQL>=0.6.6',
        'greenlet>=0.4.2',
    ],
    author='snower',
    author_email='sujian199@gmail.com',
    url='https://github.com/snower/TorMySQL.git',
    license='MIT',
    keywords=[
        "tornado", "mysql"
    ],
    description='Tornado asynchronous MySQL Driver',
    long_description=open("README.rst").read(),
    zip_safe=False,
)
