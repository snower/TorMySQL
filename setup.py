#!/usr/bin/env python

import os
from setuptools import setup

if os.path.exists("README.md"):
    with open("README.md") as fp:
        long_description = fp.readall()
else:
    long_description = ''

setup(
    name='TorMySQL',
    version='0.0.1',
    packages=['tormysql'],
    package_data={
        '': ['README.md'],
    },
    install_requires=[
        'tornado.=4.0',
        'PyMySQL>=0.6.2',
        'greenlet>=0.4.2',
    ],
    author='snower',
    author_email='sujian199@gmail.com',
    url='https://github.com/snower/TorMySQL.git',
    license='MIT',
    keywords = [
        "tornado", "mysql"
    ],
    description='Tornado高性能MySQL Driver',
    long_description= long_description,
    zip_safe=False,
)
