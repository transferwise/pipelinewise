
#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='cli',
    version='0.0.1',
    description='ETLWise Command Line Interface',
    author='xyz',
    url='xzy',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['cli'],
    install_requires=[
        'argparse==1.4.0'
    ],
    entry_points='''
        [console_scripts]
        etlwise=cli:main
    ''',
    packages=['cli']
)