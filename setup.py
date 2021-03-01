# -*- coding: utf8 -*-

from setuptools import setup


setup(
    name='clickhouse-driver-decorators',
    version='0.1.1',
    description='Bunch of decorators to decorate clickhouse-driver\'s execute method',
    author='Islam Israfilov (Islam93)',
    author_email='israfilov93@gmail.com',
    license='MIT',

    python_requires='>=3.5',
    packages=['clickhouse_driver_decorators'],

    install_requires=[
        'pytz>=2021.1',
        'pandas>=0.25.3',
        'clickhouse-driver>=0.2.0'
    ],
    extras_require={
        'dev': [
            'pytest>=6.1.2',
            'pytest-cov>=2.11.1',
            'mock>=3.0.5',
            'pylint>=2.6.0',
        ],
        'test': [
            'pytest>=6.1.2',
            'pytest-cov>=2.11.1',
            'mock>=3.0.5',
        ],
    },
)
