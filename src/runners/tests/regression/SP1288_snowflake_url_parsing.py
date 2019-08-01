"""
The installer doesn't handle snowflake URLs with a region split by .,
such as azure_account.azure_region.azure.snowflakecomputing.com
"""

import pytest

from scripts.install import parse_snowflake_url


TEST_URLS = [
    "https://account.snowflakecomputing.com",
    "account.snowflakecomputing.com",
    "account",
    "https://account.region.snowflakecomputing.com",
    "account.region.snowflakecomputing.com",
    "account.region",
    "https://azure_account.azure_region.azure.snowflakecomputing.com",
    "azure_account.azure_region.azure.snowflakecomputing.com",
    "azure_account.azure_region.azure"
]

TEST_URL_RESULTS = [
    ['account', 'us-west-2'],
    ['account', 'us-west-2'],
    ['account', None],
    ['account', 'region'],
    ['account', 'region'],
    ['account', 'region'],
    ['azure_account', 'azure_region.azure'],
    ['azure_account', 'azure_region.azure'],
    ['azure_account', 'azure_region.azure']

]


def test_url_parsing():
    for url, results in zip(TEST_URLS, TEST_URL_RESULTS):
        a, b = parse_snowflake_url(url)
        assert a == results[0]
        assert b == results[1]
