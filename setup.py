from setuptools import setup, find_packages

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='shared',
    version='1.0.0',
    author='PC',
    description='Core Library of PG: Provides standardized, high-reusability modules across the ecosystem.',
    packages=find_packages(),
    install_requires=required,
)