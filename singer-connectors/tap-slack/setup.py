#!/usr/bin/env python

from setuptools import setup, find_packages

with open('README.md') as f:
    long_description = f.read()

setup(name='pipelinewise-tap-slack',
      version='1.1.1',
      description='Singer.io tap for extracting data from the Slack Web API - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/pipelinewise-tap-slack',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=['tap_slack'],
      install_requires=[
          'pipelinewise-singer-python==3.0.2',
          'slack-sdk==3.20.0',
      ],
      extras_require={
          'test': [
              'pylint==4.0.5',
              'pytest==9.0.3',
              'pytest-cov==7.1.0',
          ]
      },
      python_requires='>=3.12.0, <3.13',
      entry_points='''
          [console_scripts]
          tap-slack=tap_slack:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_slack': [
              'schemas/*.json'
          ]
      })
