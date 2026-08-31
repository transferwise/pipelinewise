#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name='pipelinewise-tap-yugabyte',
      version='0.1.0',
      description='Singer.io tap for extracting data from YugabyteDB - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/pipelinewise-tap-yugabyte',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      python_requires=">=3.12.0, <3.13",
      install_requires=[
          'pipelinewise-singer-python==3.0.2',
          'psycopg2-binary==2.9.12',
          'strict-rfc3339==0.7',
          'simplejson==4.1.1'
      ],
      extras_require={
          "test": [
              'pytest==9.1.1',
              'pylint==4.0.6',
              'pytest-cov==7.1.0'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-yugabyte=tap_yugabyte:main
      ''',
      packages=['tap_yugabyte', 'tap_yugabyte.sync_strategies']
      )
