
#!/usr/bin/env python

from setuptools import setup

setup(name='target-postgres',
    version='0.0.1',
    description='Singer.io target for writing streams to postgres',
    author='xyz',
    url='xzy',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_postgres'],
    install_requires=[
        'singer-python==5.2.0',
    ],
    entry_points='''
        [console_scripts]
        target-postgres=target_postgres:main
    ''',
    packages=['target_postgres']
)