#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pipelinewise-transform-field',
      version='2.3.0',
      description='Singer.io simple field transformer between taps and targets - PipelineWise compatible',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="Wise",
      url='https://github.com/transferwise/pipelinewise-transform-field',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Environment :: Console',
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8'
      ],
      py_modules=['transform_field'],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'dpath==2.0.*',
      ],
      extras_require={
          'test': [
              'pytest==6.2.*',
              'pytest-cov==3.0.*',
              'pylint==2.12.*',
          ]
      },
      entry_points='''
          [console_scripts]
          transform-field=transform_field:main
      ''',
      packages=['transform_field']
      )
