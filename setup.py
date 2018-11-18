# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup( 
    name='InstaMsg',
    version='0.1.0',
    description='InstaMsg embedded python firmware for IoT devices.',
    long_description=readme,
    author='SenseGrow Inc.',
    author_email='info@sensegrow.com',
    url='https://www.sensegrow.com',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
    ]
)