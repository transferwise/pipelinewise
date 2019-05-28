#!/usr/bin/env python

from setuptools import setup

setup(name='tap-snowflake',
      version='1.0.0',
      description='Singer.io tap for extracting data from Snowflake',
      author='TransferWise',
    url="https://transferwise.com",
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_snowflake'],
      install_requires=[
          'singer-python==5.3.1',
          'snowflake-connector-python==1.7.4',
          'backoff==1.3.2',
          'pendulum==1.2.0'
      ],
      entry_points='''
          [console_scripts]
          tap-snowflake=tap_snowflake:main
      ''',
      packages=['tap_snowflake', 'tap_snowflake.sync_strategies'],
)
