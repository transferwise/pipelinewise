#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-snowflake",
    version="0.0.1",
    description="Singer.io target for Snowflake",
    author="TransferWise",
    url="https://transferwise.com",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_snowflake"],
    install_requires=[
        "singer-python==5.1.1",
        "psycopg2==2.7.5",
        "snowflake-connector-python==1.6.12",
        "boto3==1.9.33",
        "inflection==0.3.1"
    ],
    entry_points="""
    [console_scripts]
    target-snowflake=target_snowflake:main
    """,
    packages=["target_snowflake"],
    package_data = {},
    include_package_data=True,
)