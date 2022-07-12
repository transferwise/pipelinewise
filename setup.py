#!/usr/bin/env python

from setuptools import find_packages, setup

with open('README.md') as f:
    LONG_DESCRIPTION = f.read()

setup(name='pipelinewise',
      python_requires='>=3.7,<3.10',
      version='0.47.1',
      description='PipelineWise',
      long_description=LONG_DESCRIPTION,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/pipelinewise',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
      ],
      install_requires=[
          'argparse==1.4.0',
          'tabulate==0.8.9',
          'PyYAML==6.0',
          'ansible-core==2.11.8',
          'Jinja2==3.0.2',
          'joblib==1.1.0',
          'PyMySQL==1.0.2',
          'psycopg2-binary==2.8.6',
          'snowflake-connector-python[pandas]==2.7.6',
          'google-cloud-bigquery==2.31.0',
          'pipelinewise-singer-python==1.*',
          'singer-encodings==0.0.*',
          'messytables==0.15.*',
          'python-pidfile==3.0.0',
          'pymongo==3.12.3',
          'tzlocal>=2.0,<4.1',
          'slackclient==2.9.3',
          'psutil==5.8.0',
          'ujson==5.4.0',
          'dnspython==2.1.*',
          'boto3==1.21.*',
          'chardet==4.0.0',
      ],
      extras_require={
          'test': [
              'pre-commit==2.18.1',
              'flake8==4.0.1',
              'pytest==7.1.1',
              'pytest-dependency==0.4.0',
              'pytest-cov==3.0.0',
              'python-dotenv==0.19.1',
              'pylint==2.10.*',
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
              'mysql-to-bigquery=pipelinewise.fastsync.mysql_to_bigquery:main',
              'postgres-to-bigquery=pipelinewise.fastsync.postgres_to_bigquery:main',
              's3-csv-to-snowflake=pipelinewise.fastsync.s3_csv_to_snowflake:main',
              's3-csv-to-postgres=pipelinewise.fastsync.s3_csv_to_postgres:main',
              's3-csv-to-redshift=pipelinewise.fastsync.s3_csv_to_redshift:main',
              's3-csv-to-bigquery=pipelinewise.fastsync.s3_csv_to_bigquery:main',
              'mongodb-to-snowflake=pipelinewise.fastsync.mongodb_to_snowflake:main',
              'mongodb-to-postgres=pipelinewise.fastsync.mongodb_to_postgres:main',
              'mongodb-to-bigquery=pipelinewise.fastsync.mongodb_to_bigquery:main',
              'partial-mysql-to-snowflake=pipelinewise.fastsync.partialsync.mysql_to_snowflake:main',
              'partial-postgres-to-snowflake=pipelinewise.fastsync.partialsync.postgres_to_snowflake:main'
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
