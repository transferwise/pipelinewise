#!/usr/bin/env python

from setuptools import find_packages, setup

with open('README.md') as f:
    long_description = f.read()

setup(
    name='pipelinewise-target-iceberg',
    version='1.0.0',
    description='Singer.io target for loading data to Apache Iceberg tables on S3',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='PipelineWise',
    url='https://github.com/transferwise/pipelinewise-target-iceberg',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.10',
    ],
    py_modules=['target_iceberg'],
    install_requires=[
        'pipelinewise-singer-python==1.*',
        'pyiceberg[pyarrow,glue,s3fs]==0.7.*',
        'boto3>=1.21,<1.27',
        'inflection==0.5.1',
        'joblib==1.3.2',
        'pyarrow>=15.0.0',
        's3fs>=2024.2.0',
    ],
    extras_require={
        'test': [
            'pytest==7.1.1',
            'pytest-cov==4.1.0',
            'pytest-mock==3.10.0',
            'python-dotenv==0.19.1',
            'moto[s3,glue]==4.1.0',
        ]
    },
    entry_points='''
        [console_scripts]
        target-iceberg=target_iceberg:main
    ''',
    packages=find_packages(exclude=['tests*']),
)
