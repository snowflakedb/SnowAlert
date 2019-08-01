"""
The installer didn't handle snowflake URLs with a region containing '.',
such as azure_account.azure_region.azure.snowflakecomputing.com
"""

from scripts.install import parse_snowflake_url


TEST_URLS = {
    "https://account.snowflakecomputing.com": ['account', 'us-west-2'],
    "account.snowflakecomputing.com": ['account', 'us-west-2'],
    "account": ['account', None],
    "https://account.region.snowflakecomputing.com": ['account', 'region'],
    "account.region.snowflakecomputing.com": ['account', 'region'],
    "account.region": ['account', 'region'],
    "https://azure_account.azure_region.azure.snowflakecomputing.com": ['azure_account', 'azure_region.azure'],
    "azure_account.azure_region.azure.snowflakecomputing.com": ['azure_account', 'azure_region.azure'],
    "azure_account.azure_region.azure": ['azure_account', 'azure_region.azure'],
}


def test_url_parsing():
    for key, value in TEST_URLS.items():
        a, b = parse_snowflake_url(key)
        assert a == value[0]
        assert b == value[1]
