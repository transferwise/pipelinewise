#!/usr/bin/env python

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='pipelinewise-tap-github',
      version='1.1.1',
      description='Singer.io tap for extracting data from the GitHub API',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/pipelinewise-tap-github',
      classifiers=[
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=['tap_github'],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'requests==2.32.4'
      ],
      extras_require={
          'test': [
              'pylint==2.10.2',
              'pytest==6.2.4'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-github=tap_github:main
      ''',
      packages=['tap_github'],
      package_data={
          'tap_github': ['tap_github/schemas/*.json']
      },
      include_package_data=True
)
