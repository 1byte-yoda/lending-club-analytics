"""Setup.py script for packaging project."""

from setuptools import setup, find_packages

import json
import os


def read_pipenv_dependencies(fname):
    """Get default dependencies from Pipfile.lock."""
    filepath = os.path.join(os.path.dirname(__file__), fname)
    with open(filepath) as lockfile:
        lockjson = json.load(lockfile)
        return [dependency for dependency in lockjson.get('default')]


def read_pip_requirements(filename: str):
    filepath = os.path.join(os.path.dirname(__file__), filename)
    with open(filepath) as f:
        return f.readlines()


if __name__ == '__main__':
    setup(
        name="lc_utils",
        version="0.1",
        package_dir={"": "az_databricks"},
        packages=find_packages("az_databricks", include=[
            "utils*"
        ]),
        description='Utility Package for Lending Club Analytics',
        install_requires=[
            *read_pip_requirements("requirements.txt"),
        ]
    )
