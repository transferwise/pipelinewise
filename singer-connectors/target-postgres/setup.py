#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-postgres",
    version="0.0.1",
    description="Singer.io target for Postgres",
    author="TransferWise",
    url="https://transferwise.com",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_postgres"],
    install_requires=[
        "singer-python==5.1.1",
        "psycopg2==2.7.5",
        "inflection==0.3.1",
        "joblib==0.13.2"
    ],
    entry_points="""
    [console_scripts]
    target-postgres=target_postgres:main
    """,
    packages=["target_postgres"],
    package_data = {},
    include_package_data=True,
)