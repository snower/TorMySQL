# -*- coding: utf-8 -*-
# 14-8-8
# create by: snower

from setuptools import setup


setup(
    name='tormysql',
    version='0.4.3',
    packages=['tormysql', 'tormysql.platform'],
    install_requires=[
        'PyMySQL>=0.7.10',
        'greenlet>=0.4.2',
    ],
    extras_require={
        "tornado": ['tornado>=5'],
    },
    author='snower, mosquito',
    author_email='sujian199@gmail.com, me@mosquito.su',
    url='https://github.com/snower/TorMySQL',
    license='MIT',
    keywords=[
        "tornado", "mysql"
    ],
    description='Tornado asynchronous MySQL Driver',
    long_description=open("README.rst").read(),
    zip_safe=False,
)
