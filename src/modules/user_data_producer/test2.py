from setuptools import setup, find_packages

setup(
    name='test',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'pyflink==1.14.6',
        # Add any other dependencies here
    ],
)