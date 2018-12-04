#!/usr/bin/env python

from setuptools import setup

setup(name='mysql-to-snowflake',
      version='0.0.1',
      description='FastSync from MySQL to Snowflake',
      author='TransferWise',
      url='https://transferwise.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['mysql_to_snowflake'],
      install_requires=[
          'attrs==16.3.0',
          'pendulum==1.2.0',
          'singer-python==5.3.1',
          'PyMySQL==0.7.11',
          'backoff==1.3.2',
          'mysql-replication==0.18',
      ],
      entry_points='''
          [console_scripts]
          mysql-to-snowflake=mysql_to_snowflake:main
      ''',
      packages=[
          'mysql_to_snowflake',
          'mysql_to_snowflake.mysql',
          'mysql_to_snowflake.mysql.sync_strategies',
          'mysql_to_snowflake.snowflake'
      ],
)