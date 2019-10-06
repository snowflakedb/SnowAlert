'''
list_aws_accounts.py

This script reflects an approach to enumerating AWS accounts using the Organizations API.
It expects to run in Lambda and write to a Snowflake database created for tracking inventory.
You may need to customize it for your environment.
'''

import boto3
import base64
import datetime
import json
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import snowflake.connector


def get_aws_client():
    sts_client = boto3.client('sts')
    response = sts_client.assume_role(
        RoleArn=os.environ['aws_audit_role'],
        RoleSessionName=os.environ['aws_audit_session_name'],
        ExternalId=os.environ['aws_audit_role_externalid'],
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
    password = kms.decrypt(
        CiphertextBlob=base64.b64decode(os.environ['private_key_password'])
    )['Plaintext'].decode()
    private_key = serialization.load_pem_private_key(
        base64.b64decode(os.environ['private_key']),
        password=password.encode(),
        backend=default_backend(),
    )
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    ctx = snowflake.connector.connect(
        user=os.environ['snowflake_user'],
        account=os.environ['snowflake_account'],
        private_key=pkb,
    )
    return ctx


def get_accounts_list(client):
    accounts = []
    paginator = client.get_paginator('list_accounts')
    page_iterator = paginator.paginate()
    for page in page_iterator:
        accounts.extend(page['Accounts'])

    accounts_list = [json.dumps(account, default=str) for account in accounts]
    return accounts_list


def load_accounts_list(sf_client, accounts_list):
    format_string = ", ".join(["(%s)"] * len(accounts_list))
    snapshotclock = datetime.datetime.utcnow().isoformat()
    sf_client.cursor().execute(
        "insert into AWS_INVENTORY.SNAPSHOTS.AWS_ACCOUNT_MAP (timestamp, account) select '"
        + snapshotclock
        + "', parse_json(column1) from values"
        + format_string,
        accounts_list,
    )


def handler(event, context):
    client = get_aws_client()
    sf_client = get_snowflake_client()
    accounts_list = get_accounts_list(client)
    load_accounts_list(sf_client, accounts_list)
