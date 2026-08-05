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
          'pendulum==3.2.0',
          'pipelinewise-singer-python==3.0.2',
          'mysql-replication==0.46',
          'PyMySQL==1.1.2',
          'plpygis==0.6.1',
          'tzlocal==5.4.4',
      ],
      extras_require={
          'test': [
              'pylint==2.13.2',
              'pytest==9.0.3',
              'pytest-cov==7.1.0'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-mysql=tap_mysql:main
      ''',
      packages=['tap_mysql', 'tap_mysql.sync_strategies'],
      )
