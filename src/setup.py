from setuptools import setup, find_packages

setup(
    name="snowalert-runners",
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'jira==2.0.0',
        'PyYAML==4.2b1',
        'snowflake-connector-python==1.7.3',
        'snowflake-sqlalchemy==1.1.2',
        'mypy==0.670',
        'mypy-extensions==0.4.1',
    ],
)
