from setuptools import setup

setup(
    name="snowalert-runners",
    version='1.0',
    packages=['runners', 'runners.helpers', 'runners.plugins'],
    install_requires=[
        'jira==2.0.0',
        'snowflake-connector-python==1.7.3',
        'snowflake-sqlalchemy==1.1.2',
        'mypy==0.650',
        'mypy-extensions==0.4.1',
    ],
)
