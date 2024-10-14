#!/usr/bin/env python

from setuptools import find_packages, setup

with open('README.md') as f:
    LONG_DESCRIPTION = f.read()

setup(name='pipelinewise',
      python_requires='==3.10.*',
      version='0.66.0',
      description='PipelineWise',
      long_description=LONG_DESCRIPTION,
      long_description_content_type='text/markdown',
      author='Wise',
      url='https://github.com/transferwise/pipelinewise',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only',
          'Programming Language :: Python :: 3.10',
      ],
      install_requires=[
          'argparse==1.4.0',
          'tabulate==0.8.9',
          'PyYAML==6.0',
          'ansible-core==2.17.5',
          'Jinja2==3.1.2',
          'joblib==1.3.2',
          'PyMySQL==0.7.11',
          'psycopg2-binary==2.9.5',
          'numpy==1.26.4',          #  numpy 2.X is not compatible with our used pandas
          'snowflake-connector-python[pandas]==3.0.4',
          'pipelinewise-singer-python==1.*',
          'python-pidfile==3.0.0',
          'pymongo==4.7.*',
          'tzlocal>=2.0,<4.1',
          'slackclient==2.9.4',
          'sqlparse==0.4.4',
          'psutil==5.9.5',
          'ujson==5.4.0',
          'dnspython==2.1.*',
          'boto3>=1.21,<1.27',
          'chardet==4.0.0',
          'backports.tarfile==1.2.0'
      ],
      extras_require={
          'test': [
              'pre-commit==2.21.0',
              'flake8==4.0.1',
              'pytest==7.1.1',
              'pytest-dependency==0.4.0',
              'pytest-cov==4.1.0',
              'python-dotenv==0.19.1',
              'pylint==2.10.*',
              'unify==0.5',
              'pytest-timer~=0.0',
          ]
      },
      entry_points={
          'console_scripts': [
              'pipelinewise=pipelinewise.cli:main',
              'mysql-to-snowflake=pipelinewise.fastsync.mysql_to_snowflake:main',
              'postgres-to-snowflake=pipelinewise.fastsync.postgres_to_snowflake:main',
              'mysql-to-postgres=pipelinewise.fastsync.mysql_to_postgres:main',
              'postgres-to-postgres=pipelinewise.fastsync.postgres_to_postgres:main',
              'mongodb-to-snowflake=pipelinewise.fastsync.mongodb_to_snowflake:main',
              'mongodb-to-postgres=pipelinewise.fastsync.mongodb_to_postgres:main',
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
