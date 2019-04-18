#!/usr/bin/env python

from setuptools import setup

setup(name='tap-s3-csv-kasparg',
      version='1.0.1',
      description='Singer.io tap for extracting CSV files from S3',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_s3_csv'],
      install_requires=[
          'backoff==1.3.2',
          'boto3==1.9.57',
          'singer-encodings==0.0.3',
          'singer-python==5.1.5',
          'voluptuous==0.10.5'
      ],
      extras_require={
          'dev': [
              'ipdb==0.11'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-s3-csv=tap_s3_csv:main
      ''',
      packages=['tap_s3_csv'])
