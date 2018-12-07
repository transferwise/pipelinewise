#!/usr/bin/env python
from setuptools import setup

setup(name='postgres-to-snowflake',
      version='0.0.1',
      description='FastSync from PostgreSQL to Snowflake',
      author='TransferWise',
      url='https://transferwise.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['postgres_to_snowflake'],
      install_requires=[
          'attrs==16.3.0',
          'idna==2.7',
          'psycopg2==2.7.5',
          "boto3==1.9.33",
          "snowflake-connector-python==1.7.2",
      ],
      entry_points='''
          [console_scripts]
          postgres-to-snowflake=postgres_to_snowflake:main
      ''',
      packages=[
          'postgres_to_snowflake'
      ],
)