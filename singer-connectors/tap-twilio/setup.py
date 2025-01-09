#!/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pipelinewise-tap-twilio',
      version='1.1.2',
      description='Singer.io tap for extracting data from the Twilio API - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/pipelinewise-tap-twilio',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=['tap_twilio'],
      install_requires=[
          'requests==2.25.*',
          'pipelinewise-singer-python==1.*'
      ],
      extras_require={
          'test': [
              'pylint==2.9.*',
              'pytest==6.2.*'
          ]
      },
      python_requires='>=3.6',
      entry_points='''
          [console_scripts]
          tap-twilio=tap_twilio:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_twilio': [
              'schemas/*.json'
          ]
      })
