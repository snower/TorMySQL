# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from setuptools import setup


setup(
    name='tormysql',
    version='0.2.5',
    packages=['tormysql'],
    install_requires=[
        'tornado>=4.1',
        'PyMySQL>=0.7.3',
        'greenlet>=0.4.2',
    ],
    author=['snower', 'mosquito'],
    author_email=['sujian199@gmail.com', 'me@mosquito.su'],
    url='https://github.com/snower/TorMySQL',
    license='MIT',
    keywords=[
        "tornado", "mysql"
    ],
    description='Tornado asynchronous MySQL Driver',
    long_description=open("README.rst").read(),
    zip_safe=False,
)
