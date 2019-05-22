#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-s3-csv",
    version="0.0.1",
    description="Singer.io target for writing CSV files and upload to S3",
    author="TransferWise",
    url="https://transferwise.com",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_s3_csv"],
    install_requires=[
        "jsonschema==2.6.0",
        "singer-python==2.1.4",
        "inflection==0.3.1",
        "boto3==1.9.57"
    ],
    entry_points="""
    [console_scripts]
    target-s3-csv=target_s3_csv:main
    """,
    packages=["target_s3_csv"],
    package_data = {},
    include_package_data=True,
)
