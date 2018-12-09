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
          'idna==2.7',
          'PyMySQL==0.7.11',
          "boto3==1.9.33",
          "snowflake-connector-python==1.7.2",
      ],
      entry_points='''
          [console_scripts]
          mysql-to-snowflake=mysql_to_snowflake:main
      ''',
      packages=[
          'mysql_to_snowflake'
      ],
)