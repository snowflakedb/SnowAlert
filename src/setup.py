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
        'snowflake-connector-python==1.8.4',
        'snowflake-sqlalchemy==1.1.2',
        'pandas==0.24.1',
        'rpy2==3.0.1',
        'pytz==2018.9',
        'slackclient==1.3.1',
        'tzlocal==1.5.1',
        'snowflake-ingest==0.9.1',
        'azure-common==1.1.20',
        'azure-mgmt-storage==3.3.0',
        'azure-mgmt-subscription==0.4.1',
        'azure-storage-blob==1.5.0',
        'azure-storage-common==1.4.0',
        'boto3'
    ],
)
