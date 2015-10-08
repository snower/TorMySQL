# encoding: utf-8
from setuptools import setup


setup(
    name='tormysql',
    version='0.2.3',
    packages=['tormysql'],
    install_requires=[
        'tornado>=4.1',
        'PyMySQL==0.6.6',
        'greenlet>=0.4.2',
    ],
    author=['snower', 'mosquito'],
    author_email=['sujian199@gmail.com', 'me@mosquito.su'],
    url='https://github.com/mosquito/tormysql.git',
    license='MIT',
    keywords=[
        "tornado", "mysql"
    ],
    description='Tornado asynchronous MySQL Driver [fork of TorMySQL]',
    long_description=open("README.rst").read(),
    zip_safe=False,
)
