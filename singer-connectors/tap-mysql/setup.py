#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pipelinewise-tap-mysql',
      version='1.6.0',
      description='Singer.io tap for extracting data from MySQL & MariaDB - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/pipelinewise-tap-mysql',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=['tap_mysql'],
      install_requires=[
          'pendulum==2.1.2',
          'pipelinewise-singer-python==2.*',
          'mysql-replication==0.43',
          'PyMySQL==1.1.*',
          'plpygis==0.2.1',
      ],
      extras_require={
          'test': [
              'nose==1.3.*',
              'pylint==2.13.2',
              'nose-cov==1.6'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-mysql=tap_mysql:main
      ''',
      packages=['tap_mysql', 'tap_mysql.sync_strategies'],
      )
