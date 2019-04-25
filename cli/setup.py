
#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='cli',
    version='0.0.1',
    description='PipelineWise Command Line Interface',
    author='xyz',
    url='xzy',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['cli'],
    install_requires=[
        'argparse==1.4.0',
        'python-crontab==2.3.5',
        'tabulate==0.8.2',
        'PyYAML==5.1.0',
        'jsonschema==3.0.1',
        'ansible-vault==1.2.0',
        'joblib==0.13.2'
    ],
    entry_points='''
        [console_scripts]
        pipelinewise=cli:main
    ''',
    packages=['cli']
)