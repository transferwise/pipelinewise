#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
      long_description = f.read()

setup(name='pipelinewise-tap-postgres',
      version='1.1.2',
      description='Singer.io tap for extracting data from PostgreSQL - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Stitch',
      url='https://github.com/transferwise/pipelinewise-tap-postgres',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      install_requires=[
          'singer-python==5.3.1',
          'psycopg2==2.8.2',
          'strict-rfc3339==0.7',
          'nose==1.3.7'
      ],
      entry_points='''
          [console_scripts]
          tap-postgres=tap_postgres:main
      ''',
      packages=['tap_postgres', 'tap_postgres.sync_strategies']
)
