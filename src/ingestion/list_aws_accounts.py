import boto3
import base64
import datetime
import json
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

import snowflake.connector

def get_aws_client():
    sts_client_source = boto3.client('sts')
    sts_client_source_response = sts_client_source.assume_role(RoleArn=os.environ['LIST_ACCOUNTS_AWS_AUDIT_SOURCE_ROLE_ARN'],RoleSessionName=os.environ['LIST_ACCOUNTS_AWS_AUDIT_SOURCE_ROLE_SESSION_NAME'])
    sts_client_destination = boto3.Session(aws_access_key_id=sts_client_source_response['Credentials']['AccessKeyId'],aws_secret_access_key=sts_client_source_response['Credentials']['SecretAccessKey'],aws_session_token=sts_client_source_response['Credentials']['SessionToken'])
    response = sts_client_destination.client('sts').assume_role(RoleArn=os.environ['LIST_ACCOUNTS_AWS_AUDIT_DESTINATION_ROLE_ARN'],RoleSessionName=os.environ['LIST_ACCOUNTS_AWS_AUDIT_DESTINATION_ROLE_SESSION_NAME'],ExternalId=os.environ['LIST_ACCOUNTS_AWS_AUDIT_DESTINATION_ROLE_ARN_EXTERNALID'])
    org_session = boto3.Session(aws_access_key_id=response['Credentials']['AccessKeyId'],aws_secret_access_key=response['Credentials']['SecretAccessKey'],aws_session_token=response['Credentials']['SessionToken'])
    org_client = org_session.client('organizations')
    return org_client

def get_snowflake_client():
    kms = boto3.client('kms')
    password = kms.decrypt(CiphertextBlob=base64.b64decode(os.environ['LIST_ACCOUNTS_SNOWFLAKE_USER_PRIVATE_KEY_PASSWORD']))['Plaintext'].decode()
    private_key = serialization.load_pem_private_key(base64.b64decode(os.environ['LIST_ACCOUNTS_SNOWFLAKE_USER_PRIVATE_KEY']), password=password.encode(), backend=default_backend())
    pkb = private_key.private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.TraditionalOpenSSL, encryption_algorithm=serialization.NoEncryption())
    ctx = snowflake.connector.connect(user=os.environ['LIST_ACCOUNTS_SNOWFLAKE_USER'], account=os.environ['LIST_ACCOUNTS_SNOWFLAKE_ACCOUNT'], private_key=pkb)
    return ctx

def get_accounts_list(client):
    accounts = []
    paginator = client.get_paginator('list_accounts')
    page_iterator = paginator.paginate()
    for page in page_iterator:
        accounts.extend(page['Accounts'])

    accounts_list = [json.dumps(account, default=str) for account in accounts]
    return accounts_list

def load_accounts_list(sf_client,accounts_list):
    format_string = ", ".join(["(%s)"]*len(accounts_list))
    snapshotclock = datetime.datetime.utcnow().isoformat()
    sf_client.cursor().execute("insert into " + os.environ['LIST_ACCOUNTS_SNOWFLAKE_TABLE_IDENTIFIER'] + " (timestamp, account) select '" + snapshotclock + "', parse_json(column1) from values" + format_string, (accounts_list))

def main():
    current_time = datetime.datetime.now()
    if current_time.hour = 1 and current_time.minute > 0 and current_time.minute <= 15:
        client = get_aws_client()
        sf_client = get_snowflake_client()
        accounts_list=get_accounts_list(client)
        load_accounts_list(sf_client,accounts_list)
    else:
        print("It's not time yet!")

main()
