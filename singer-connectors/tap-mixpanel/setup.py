#!/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pipelinewise-tap-mixpanel',
      version='1.2.0',
      description='Singer.io tap for extracting data from the mixpanel API - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='jeff.huth@bytecode.io & TransferWise',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=['tap_mixpanel'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.22.0',
          'pipelinewise-singer-python==1.*',
          'jsonlines==1.2.0'
      ],
      extras_require={
        'test': [
            'pylint==2.9.*',
            'pytest==6.2.*',
            'requests_mock==1.9.*',
        ]
      },
      entry_points='''
          [console_scripts]
          tap-mixpanel=tap_mixpanel:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_mixpanel': [
              'schemas/*.json'
          ]
      })
