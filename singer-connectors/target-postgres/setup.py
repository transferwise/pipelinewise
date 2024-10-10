#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name="pipelinewise-target-postgres",
      version="2.1.2",
      description="Singer.io target for loading data to PostgreSQL - PipelineWise compatible",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="TransferWise",
      url='https://github.com/transferwise/pipelinewise-target-postgres',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=["target_postgres"],
      install_requires=[
          'pipelinewise-singer-python==2.*',
          'psycopg2-binary==2.9.5',
          'inflection==0.3.1',
          'joblib==1.2.0',
      ],
      extras_require={
          "test": [
              'pytest==6.2.5',
              'pylint==2.6.0',
              'pytest-cov==2.10.1',
          ]
      },
      entry_points="""
          [console_scripts]
          target-postgres=target_postgres:main
      """,
      packages=["target_postgres"],
      package_data={},
      include_package_data=True,
      )
