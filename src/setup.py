from setuptools import setup, find_packages

setup(
    name="snowalert-runners",
    version='1.0',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'aiohttp[speedups]',
        'aioboto3==8.1.1',
        'fire==0.3.1',
        'jira==2.0.0',
        'PyYAML==5.3.1',
        'xmltodict==0.12.0',
        'snowflake-connector-python==2.3.6',
        'snowflake-sqlalchemy==1.2.3',
        'pandas==1.0.4',
        'pybrake==0.4.0',
        'pytz==2018.9',
        'slackclient==1.3.1',
        'tzlocal==1.5.1',
        'azure-common==1.1.23',
        'azure-mgmt-compute==8.0.0',
        'azure-mgmt-network==5.0.0',
        'azure-mgmt-storage==4.1.0',
        'azure-mgmt-subscription==0.5.0',
        'azure-storage-blob==12.0.0',
        'azure-storage-common==2.1.0',
        'google-api-python-client==1.8.2',
        'pyTenable==1.1.1',
        'boto3',
        'botocore',
        'twilio==6.29.4',
        'simple_salesforce==0.74.3',
        'sentry-sdk==0.17.7',
        'pdpyras==4.0',
        'duo_client==4.2.3',
        'cryptography==3.0.0',
        'requests==2.23.0',
        'pymsteams==0.1.14',
    ],
    extras_require={
        'dev': [
            'python-dotenv',
            'pytest',
            'black',
            'mypy',
            'python-dotenv',
            'pyflakes',
            'pytest',
            'pytest-ordering',
            'mypy',
            'mypy-extensions',
        ]
    },
)
