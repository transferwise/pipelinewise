#!/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pipelinewise-tap-mixpanel',
      version='1.7.1',
      description='Singer.io tap for extracting data from the mixpanel API - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/pipelinewise',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=['tap_mixpanel'],
      install_requires=[
          'backoff==2.2.1',
          'requests==2.32.3',
          'singer-python==6.0.0',
          'jsonlines==1.2.0'
      ],
      extras_require={
          'test': [
              'pytest==6.2.*',
              'requests_mock==1.9.*',
          ]
      },
      python_requires='>=3.8',
      include_package_data=True,
      zip_safe=False,
      project_urls={
          'Source': 'https://github.com/transferwise/pipelinewise',
          'Issues': 'https://github.com/transferwise/pipelinewise/issues',
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
