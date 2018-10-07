
#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='rest-api',
    version='0.0.1',
    description='AnalyticsDB ETL RESTful API',
    author='xyz',
    url='xzy',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['rest_api'],
    install_requires=[
        'Flask==1.0.2',
        'Flask-Script==2.0.6',
        'flask-cors==3.0.6',
        'python-crontab==2.3.5'
    ],
    entry_points='''
        [console_scripts]
        rest-api=rest_api:main
    ''',
    packages=['rest_api']
)