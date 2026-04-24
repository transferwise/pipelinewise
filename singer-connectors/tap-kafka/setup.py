#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pipelinewise-tap-kafka',
      version='8.2.1',
      description='Singer.io tap for extracting data from Kafka topic - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='TransferWise',
      url='https://github.com/transferwise/pipelinewise-tap-kafka',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      install_requires=[
          'pipelinewise-singer-python==3.0.2',
          'dpath==2.1.*',
          'confluent-kafka[protobuf]==2.3.*',
          'grpcio-tools==1.80.*'
      ],
      extras_require={
          'test': [
              'pytest==9.0.3',
              'pylint==4.0.5',
              'pytest-cov==7.1.0'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-kafka=tap_kafka:main
      ''',
      packages=['tap_kafka', 'tap_kafka.serialization']
)
