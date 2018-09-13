
#!/usr/bin/env python

from setuptools import setup

setup(name='transform-field',
    version='0.0.1',
    description='Singer.io simple field transformator between taps and targets',
    author='xyz',
    url='xzy',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['transform_field'],
    install_requires=[
        'singer-python==5.2.0',
    ],
    entry_points='''
        [console_scripts]
        transform-field=transform_field:main
    ''',
    packages=['transform_field']
)