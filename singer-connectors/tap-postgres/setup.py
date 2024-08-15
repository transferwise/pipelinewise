#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name='pipelinewise-tap-postgres',
      version='2.1.0',
      description='Singer.io tap for extracting data from PostgresSQL - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/pipelinewise-tap-postgres',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      python_requires=">=3.7,<3.10",
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'psycopg2-binary==2.9.5',
          'strict-rfc3339==0.7',
      ],
      extras_require={
          "test": [
              'pytest==7.2.2',
              'pylint==2.12.*',
              'pytest-cov==4.0.0'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-postgres=tap_postgres:main
      ''',
      packages=['tap_postgres', 'tap_postgres.sync_strategies']
      )
