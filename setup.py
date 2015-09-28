# encoding: utf-8
from setuptools import setup


setup(
    name='mytor',
    version='0.2.1',
    packages=['mytor'],
    install_requires=[
        'tornado>=4.1',
        'PyMySQL>=0.6.6',
        'greenlet>=0.4.2',
    ],
    author='snower',
    author_email='sujian199@gmail.com',
    url='https://github.com/mosquito/mytor.git',
    license='MIT',
    keywords=[
        "tornado", "mysql"
    ],
    description='Tornado asynchronous MySQL Driver [fork of TorMySQL]',
    long_description=open("README.rst").read(),
    zip_safe=False,
)
