#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from setuptools import setup

if os.path.exists("README.md"):
    with open("README.md") as fp:
        long_description = fp.read()
else:
    long_description = ''

setup(
    name='TorMySQL',
    version='0.0.7',
    packages=['tormysql'],
    package_data={
        '': ['README.md'],
    },
    install_requires=[
        'tornado>=4.1',
        'PyMySQL>=0.6.3',
        'greenlet>=0.4.2',
    ],
    author='snower',
    author_email='sujian199@gmail.com',
    url='https://github.com/snower/TorMySQL.git',
    license='MIT',
    keywords = [
        "tornado", "mysql"
    ],
    description='Tornado asynchronous MySQL Driver',
    long_description= long_description,
    zip_safe=False,
)
