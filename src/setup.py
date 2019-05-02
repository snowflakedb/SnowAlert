from setuptools import setup, find_packages

setup(
    name="snowalert-runners",
    version='1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'fire==0.1.3',
        'jira==2.0.0',
        'PyYAML==4.2b1',
        'snowflake-connector-python==1.7.8',
        'snowflake-sqlalchemy==1.1.2',
        'pandas==0.24.1',
        'rpy2==3.0.1',
        'pytz==2018.9',
        'slackclient==1.3.1',
    ],
)
