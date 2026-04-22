#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name="pipelinewise-target-s3-csv",
      version="2.0.0",
      python_requires=">=3.12.0, <3.13",
      description="Singer.io target for writing CSV files and upload to S3 - PipelineWise compatible",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="Wise",
      url='https://github.com/transferwise/pipelinewise-target-s3-csv',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: 3.12',
      ],
      py_modules=["target_s3_csv"],
      install_requires=[
          'pipelinewise-singer-python==3.0.2',
          'inflection==0.5.1',
          'boto3==1.17.39',
      ],
      extras_require={
          "test": [
              'pylint==4.0.5',
              'pytest==9.0.3',
              'pytest-cov==7.1.0',
          ]
      },
      entry_points="""
          [console_scripts]
          target-s3-csv=target_s3_csv:main
       """,
      packages=["target_s3_csv"],
      package_data={},
      include_package_data=True,
      )
