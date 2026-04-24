#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name='pipelinewise-tap-postgres',
      version='2.2.0',
      description='Singer.io tap for extracting data from PostgresSQL - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/pipelinewise-tap-postgres',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      python_requires=">=3.12.0, <3.13",
      install_requires=[
          'pipelinewise-singer-python==3.0.2',
          'psycopg2-binary==2.9.12',
          'strict-rfc3339==0.7',
          'simplejson==4.0.1'
      ],
      extras_require={
          "test": [
              'pytest==9.0.3',
              'pylint==4.0.5',
              'pytest-cov==7.1.0'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-postgres=tap_postgres:main
      ''',
      packages=['tap_postgres', 'tap_postgres.sync_strategies']
      )
