"""
The installer didn't handle snowflake URLs with a region containing '.',
such as azure_account.azure_region.azure.snowflakecomputing.com
"""

from scripts.install import parse_snowflake_url


TEST_URLS = {
    "account": ['account', None],
    "account.snowflakecomputing.com": ['account', 'us-west-2'],
    "account.region": ['account', 'region'],
    "account.region.snowflakecomputing.com": ['account', 'region'],
    "azure_account.azure_region.azure": ['azure_account', 'azure_region.azure'],
    "azure_account.azure_region.azure.snowflakecomputing.com": [
        'azure_account',
        'azure_region.azure',
    ],
    "https://account.snowflakecomputing.com": ['account', 'us-west-2'],
    "https://account.region.snowflakecomputing.com": ['account', 'region'],
    "https://azure_account.azure_region.azure.snowflakecomputing.com/console": [
        'azure_account',
        'azure_region.azure',
    ],
}


def test_url_parsing():
    for key, value in TEST_URLS.items():
        a, b = parse_snowflake_url(key)
        assert a == value[0]
        assert b == value[1]
