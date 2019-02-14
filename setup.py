# -*- coding: utf-8 -*-

import sys

if sys.version_info < (3, 0):
    print('\nInstaMsg requires at least Python 3.0!')
    sys.exit(1)

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup( 
    name='InstaMsg',
    version='1.1.1',
    description='InstaMsg python library for IoT devices.',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='SenseGrow Inc.',
    author_email='info@sensegrow.com',
    url='https://www.sensegrow.com',
    license=license,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=[
        'wolfssl>=3.14.0-1',
        'websocket-client>=0.54.0'
    ],
    classifiers=[
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)