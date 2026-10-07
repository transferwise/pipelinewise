#!/usr/bin/env python

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name="pipelinewise-singer-python",
      version='3.0.2+pipelinewise.0.94.0',
      description="Singer.io utility library - PipelineWise compatible",
      python_requires=">=3.12, <3.13",
      long_description=long_description,
      long_description_content_type="text/markdown",
      author="TransferWise",
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      url="https://github.com/transferwise/pipelinewise-singer-python",
      setup_requires=[
        'wrapt>=1.14.0',
      ],
      install_requires=[
          'wrapt>=1.14.0',
          'pytz',
          'jsonschema==3.2.0',
          'orjson==3.11.8',
          'python-dateutil>=2.6.0',
          'backoff==2.1.2',
          'ciso8601',
      ],
      extras_require={
          'test': [
              'ruff==0.16.1',
              'pytest==9.0.3',
              'pytest-cov==7.1.0',
          ]
      },
      packages=['singer'],
      package_data={
          'singer': [
              'logging.conf'
          ]
      },
      include_package_data=True
      )
