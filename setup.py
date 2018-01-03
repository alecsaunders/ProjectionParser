# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

setup(
    name='ProjectionParser',
    version='0.1.0',
    description='Python package to parse Vertica projections',
    long_description=readme,
    author='Alec Saunders',
    author_email='alec.saunders@domo.com',
    url='https://git.empdev.domo.com/DBA/ProjectionParser',
)
