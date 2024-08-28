#!/usr/bin/env python

from setuptools import setup

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(name='pipelinewise-tap-zendesk',
      version='1.2.1',
      description='Singer.io tap for extracting data from the Zendesk API',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='TransferWise',
      url='https://github.com/transferwise/pipelinewise-tap-zendesk',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_zendesk'],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'zenpy==2.0.24',
      ],
      extras_require={
          'test': [
              'ipdb==0.13.*',
              'pylint==2.9.*',
              'pytest==6.2.*',
              'pytest-cov==2.12.*',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-zendesk=tap_zendesk:main
      ''',
      packages=['tap_zendesk'],
      include_package_data=True,
)
