# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='stock36',
    version='0.1.0',
    description='Stock Utilities under Python 3.6',
    long_description=readme,
    author='Desheng Xu',
    author_email='xudesheng@gmail.com',
    url='https://github.com/xudesheng/stock36',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)

