#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name='pipelinewise-tap-s3-csv',
      version='3.0.1',
      description='Singer.io tap for extracting CSV files from S3 - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='TransferWise',
      url='https://github.com/transferwise/pipelinewise-tap-s3-csv',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=['tap_s3_csv'],
      install_requires=[
          'boto3==1.26.165',
          'singer-encodings==0.0.*',
          'pipelinewise-singer-python==3.0.2',
          'voluptuous==0.13.1',
          'ujson==5.12.0',
          'more_itertools==8.12.*',
      ],
      extras_require={
          'dev': [
              'ipdb==0.13.9',
          ],
          'test': [
              'pytest==9.0.3',
              'pylint==4.0.5',
              'pytest-cov==7.1.0'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-s3-csv=tap_s3_csv:main
      ''',
      packages=['tap_s3_csv'])
