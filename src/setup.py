from setuptools import setup, find_packages

setup(
    name="snowalert-runners",
    version='1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'pytest==4.3.1',
        'pytest-ordering==0.6',
        'jira==2.0.0',
        'PyYAML==4.2b1',
        'fire==0.1.3',
        'snowflake-connector-python==1.7.8',
        'snowflake-sqlalchemy==1.1.2',
        'mypy==0.670',
        'mypy-extensions==0.4.1',
    ],
)
