#!/usr/bin/env python

from setuptools import find_packages, setup

with open('README.md') as f:
    LONG_DESCRIPTION = f.read()

setup(name='pipelinewise',
      version='0.29.0',
      description='PipelineWise',
      long_description=LONG_DESCRIPTION,
      long_description_content_type='text/markdown',
      author='TransferWise',
      url='https://github.com/transferwise/pipelinewise',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      install_requires=[
          'argparse==1.4.0',
          'tabulate==0.8.2',
          'PyYAML==5.3.1',
          'ansible==2.7.16',
          'Jinja2==2.11.2',
          'joblib==1.0.0',
          'PyMySQL==0.7.11',
          'psycopg2-binary==2.8.5',
          'snowflake-connector-python[pandas]==2.3.7',
          'pipelinewise-singer-python==1.*',
          'singer-encodings==0.0.*',
          'python-dateutil<2.8.1',
          'messytables==0.15.*',
          'python-pidfile==3.0.0',
          'pre-commit==2.9.3',
          'pymongo==3.10.*',
          'tzlocal==2.0.*',
          'slackclient>=2.7,<2.10',
          'psutil==5.8.0'
      ],
      extras_require={
          'test': [
              'pytest==6.2.1',
              'pytest-dependency==0.4.0',
              'coverage==4.5.3',
              'python-dotenv==0.15.0',
              'mock==4.0.3',
              'pylint==2.6.0',
              'unify==0.5'
          ]
      },
      entry_points={
          'console_scripts': [
              'pipelinewise=pipelinewise.cli:main',
              'mysql-to-snowflake=pipelinewise.fastsync.mysql_to_snowflake:main',
              'postgres-to-snowflake=pipelinewise.fastsync.postgres_to_snowflake:main',
              'mysql-to-redshift=pipelinewise.fastsync.mysql_to_redshift:main',
              'postgres-to-redshift=pipelinewise.fastsync.postgres_to_redshift:main',
              'mysql-to-postgres=pipelinewise.fastsync.mysql_to_postgres:main',
              'postgres-to-postgres=pipelinewise.fastsync.postgres_to_postgres:main',
              's3-csv-to-snowflake=pipelinewise.fastsync.s3_csv_to_snowflake:main',
              's3-csv-to-postgres=pipelinewise.fastsync.s3_csv_to_postgres:main',
              's3-csv-to-redshift=pipelinewise.fastsync.s3_csv_to_redshift:main',
              'mongodb-to-snowflake=pipelinewise.fastsync.mongodb_to_snowflake:main',
              'mongodb-to-postgres=pipelinewise.fastsync.mongodb_to_postgres:main',
          ]
      },
      packages=find_packages(exclude=['tests*']),
      package_data={
          'schemas': [
              'pipelinewise/cli/schemas/*.json'
          ],
          'pipelinewise': [
              'logging.conf',
              'logging_debug.conf'
          ]
      },
      include_package_data=True)
