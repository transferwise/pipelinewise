#!/usr/bin/env python

from setuptools import find_packages, setup

with open('README.md') as f:
    long_description = f.read()

setup(name="pipelinewise-target-snowflake",
      version="2.4.0",
      description="Singer.io target for loading data to Snowflake - PipelineWise compatible",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="Wise",
      url='https://github.com/transferwise/pipelinewise-target-snowflake',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
      ],
      py_modules=["target_snowflake"],
      python_requires='>=3.7',
      install_requires=[
          'pipelinewise-singer-python==2.*',
          'numpy==1.26.4',         #  numpy 2.X is not compatible with our used pandas
          'snowflake-connector-python[pandas]==3.15.0',
          'inflection==0.5.1',
          'joblib==1.2.0',
          'boto3==1.28.20',
      ],
      extras_require={
          "test": [
              "pylint==2.12.*",
              'pytest==7.4.0',
              'pytest-cov==3.0.0',
              "python-dotenv>=0.19,<1.1"
          ]
      },
      entry_points="""
          [console_scripts]
          target-snowflake=target_snowflake:main
      """,
      packages=find_packages(exclude=['tests*']),
      )
