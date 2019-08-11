
#!/usr/bin/env python

from setuptools import setup, find_packages

with open('README.md') as f:
      long_description = f.read()

setup(name='pipelinewise',
    version='0.9.1',
    description='PipelineWise',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="TransferWise",
    url='https://github.com/transferwise/pipelinewise',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3 :: Only'
    ],
    install_requires=[
        'argparse==1.4.0',
        'python-crontab==2.3.5',
        'tabulate==0.8.2',
        'PyYAML==5.1.0',
        'jsonschema==3.0.1',
        'ansible==2.7.10',
        'joblib==0.13.2',
        
        'attrs==17.4.0',
        'idna==2.7',
        'PyMySQL==0.7.11',
        'psycopg2==2.8.2',
        'boto3==1.9.33',
        'snowflake-connector-python==1.7.2'
    ],
    extras_require={
        "test": [
            "pytest==5.0.1",
            "coverage==4.5.3"
        ]
    },
    entry_points='''
        [console_scripts]
        pipelinewise=pipelinewise.pipelinewise:main
        mysql-to-snowflake=pipelinewise.mysql_to_snowflake:main
        postgres-to-snowflake=pipelinewise.postgres_to_snowflake:main
        mysql-to-redshift=pipelinewise.mysql_to_redshift:main
        postgres-to-redshift=pipelinewise.postgres_to_redshift:main
    ''',
    packages=find_packages(exclude=['tests*']),
    package_data = {
        "schemas": ["pipelinewise/cli/schemas/*.json"]
    },
    include_package_data=True
)
