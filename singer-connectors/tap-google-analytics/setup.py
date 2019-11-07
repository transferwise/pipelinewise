#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-google-analytics",
    version="0.1.0",
    description="Singer.io tap for extracting data from the Google Analytics Reporting API",
    author='Meltano Team & Contributors',
    author_email="meltano@gitlab.com",
    url="https://gitlab.com/meltano/tap-google-analytics",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_google_analytics"],
    install_requires=[
        "singer-python==5.6.1",
        "google-api-python-client==1.7.9",
        "oauth2client==4.1.3",
        "backoff==1.3.2"
    ],
    entry_points="""
    [console_scripts]
    tap-google-analytics=tap_google_analytics:main
    """,
    packages=["tap_google_analytics"],
    package_data = {
      'tap_google_analytics/defaults': [
        "default_report_definition.json",
      ],
    },
    include_package_data=True,
)
