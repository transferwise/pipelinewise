#!/usr/bin/env python

from setuptools import setup

setup(name='tap-kafka',
      version='0.0.1',
      description='Singer.io tap for extracting data from kafka',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      install_requires=[
	  'python-snappy',
          'kafka-python',
          'singer-python==5.2.0',
          'requests==2.12.4',
	  'psycopg2==2.7.4',
	  'strict-rfc3339==0.7',
	  'nose==1.3.7',
          'jsonschema==2.6.0',

      ],
      entry_points='''
          [console_scripts]
          tap-kafka=tap_kafka:main
      ''',
      packages=['tap_kafka']
)
