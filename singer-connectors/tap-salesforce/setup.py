#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name='pipelinewise-tap-salesforce',
      version='1.1.0',
      description='Singer.io tap for extracting data from the Salesforce API - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',  # This is important!
      author='TransferWise',
      url='https://github.com/transferwise/pipelinewise-tap-salesforce',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=['tap_salesforce'],
      install_requires=[
          'requests==2.20.0',
          'pipelinewise-singer-python==1.*',
          'xmltodict==0.11.0'
      ],
      extras_require={
          'test': [
              'pylint==2.9.*',
          ]
      },
      python_requires='>=3.6',
      entry_points='''
          [console_scripts]
          tap-salesforce=tap_salesforce:main
      ''',
      packages=['tap_salesforce', 'tap_salesforce.salesforce'],
      package_data={
          'tap_salesforce/schemas': [
              # add schema.json filenames here
          ]
      },
      include_package_data=True,
      )
