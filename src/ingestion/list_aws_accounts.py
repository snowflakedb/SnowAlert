import boto3
import base64
import datetime
import json
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import snowflake.connector

AWS_ACCOUNTS_TABLE = os.environ['LIST_ACCOUNTS_SNOWFLAKE_TABLE_IDENTIFIER']
SA_ACCOUNT = os.environ['INGEST_SNOWFLAKE_ACCOUNT']
SA_USER = os.environ['INGEST_SNOWFLAKE_USER']
SA_USER_PK = os.environ['INGEST_SNOWFLAKE_USER_PRIVATE_KEY']
SA_USER_PK_PASSWORD = os.environ['INGEST_SNOWFLAKE_USER_PRIVATE_KEY_PASSWORD']


def get_aws_client():
    sts_client_source = boto3.client('sts')
    sts_client_source_response = sts_client_source.assume_role(
        RoleArn=os.environ['AWS_AUDIT_SOURCE_ROLE_ARN'],
        RoleSessionName=os.environ['LIST_ACCOUNTS_AWS_AUDIT_SOURCE_ROLE_SESSION_NAME'],
    )
    sts_client_destination = boto3.Session(
        aws_access_key_id=sts_client_source_response['Credentials']['AccessKeyId'],
        aws_secret_access_key=sts_client_source_response['Credentials'][
            'SecretAccessKey'
        ],
        aws_session_token=sts_client_source_response['Credentials']['SessionToken'],
    )
    sts = sts_client_destination.client('sts')
    response = sts.assume_role(
        RoleArn=os.environ['AWS_AUDIT_DESTINATION_ROLE_ARN'],
        RoleSessionName=os.environ[
            'LIST_ACCOUNTS_AWS_AUDIT_DESTINATION_ROLE_SESSION_NAME'
        ],
        ExternalId=os.environ['AWS_AUDIT_DESTINATION_ROLE_ARN_EXTERNALID'],
    )
    org_session = boto3.Session(
        aws_access_key_id=response['Credentials']['AccessKeyId'],
        aws_secret_access_key=response['Credentials']['SecretAccessKey'],
        aws_session_token=response['Credentials']['SessionToken'],
    )
    org_client = org_session.client('organizations')
    return org_client


def get_snowflake_client():
    kms = boto3.client('kms')
    password = kms.decrypt(CiphertextBlob=base64.b64decode(SA_USER_PK_PASSWORD))[
        'Plaintext'
    ].decode()

    private_key = serialization.load_pem_private_key(
        base64.b64decode(SA_USER_PK),
        password=password.encode(),
        backend=default_backend(),
    )
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    ctx = snowflake.connector.connect(user=SA_USER, account=SA_ACCOUNT, private_key=pkb)
    return ctx


def get_accounts_list(client):
    accounts = []
    paginator = client.get_paginator('list_accounts')
    page_iterator = paginator.paginate()
    for page in page_iterator:
        accounts.extend(page['Accounts'])

    accounts_list = [json.dumps(account, default=str) for account in accounts]
    return accounts_list


LOAD_ACCOUNTLIST_QUERY = f"""
INSERT INTO {AWS_ACCOUNTS_TABLE} (timestamp, account)
SELECT '{{snapshotclock}}'::timestamp_ltz, PARSE_JSON(column1)
FROM VALUES {{format_string}}
"""


def load_accounts_list(sf_client, accounts_list):
    query = LOAD_ACCOUNTLIST_QUERY.format(
        snapshotclock=datetime.datetime.utcnow().isoformat(),
        format_string=", ".join(["(%s)"] * len(accounts_list)),
    )
    sf_client.cursor().execute(query, accounts_list)


def main():
    sf_client = get_snowflake_client()
    current_time = datetime.datetime.now(datetime.timezone.utc)
    last_time = (
        sf_client.cursor()
        .execute(f'SELECT max(timestamp) FROM {AWS_ACCOUNTS_TABLE}')
        .fetchall()[0][0]
    )
    if (current_time - last_time).total_seconds() > 86400:
        client = get_aws_client()
        accounts_list = get_accounts_list(client)
        load_accounts_list(sf_client, accounts_list)
    else:
        print("It's not time yet!")


if __name__ == '__main__':
    main()
