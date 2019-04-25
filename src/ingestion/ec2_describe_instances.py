import boto3
import base64
import datetime
import json
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from multiprocessing import Pool
from runners.utils import groups_of

import snowflake.connector

INSTANCES_TABLE = os.environ['EC2_INSTANCE_LIST_SNOWFLAKE_TABLE_IDENTIFIER']
AWS_ACCOUNTS_TABLE = os.environ['LIST_ACCOUNTS_SNOWFLAKE_TABLE_IDENTIFIER']
AWS_AUDIT_ROLE_NAME = os.environ['AWS_AUDIT_DESTINATION_ROLE_NAME']
SA_ACCOUNT = os.environ['INGEST_SNOWFLAKE_ACCOUNT']
SA_USER = os.environ['INGEST_SNOWFLAKE_USER']
SA_USER_PK = os.environ['INGEST_SNOWFLAKE_USER_PRIVATE_KEY']
SA_USER_PK_PASSWORD = os.environ['INGEST_SNOWFLAKE_USER_PRIVATE_KEY_PASSWORD']
CACHED_AWS_CLIENT = None


def get_snowflake_client():
    kms = boto3.client('kms')
    password = kms.decrypt(CiphertextBlob=base64.b64decode(SA_USER_PK_PASSWORD))['Plaintext'].decode()

    private_key = serialization.load_pem_private_key(
        base64.b64decode(SA_USER_PK),
        password=password.encode(),
        backend=default_backend()
    )
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
    )
    ctx = snowflake.connector.connect(
        user=SA_USER,
        account=SA_ACCOUNT,
        private_key=pkb
    )
    return ctx


def get_cached_aws_client():
    sts_client_source = boto3.client('sts')
    sts_client_source_response = sts_client_source.assume_role(
        RoleArn=os.environ['AWS_AUDIT_SOURCE_ROLE_ARN'],
        RoleSessionName=os.environ['EC2_INSTANCE_LIST_AWS_AUDIT_SOURCE_ROLE_SESSION_NAME']
    )
    sts_client_destination = boto3.Session(
        aws_access_key_id=sts_client_source_response['Credentials']['AccessKeyId'],
        aws_secret_access_key=sts_client_source_response['Credentials']['SecretAccessKey'],
        aws_session_token=sts_client_source_response['Credentials']['SessionToken']
    )
    sts_client = sts_client_destination.client('sts')
    return sts_client


def get_aws_client(account):
    global CACHED_AWS_CLIENT
    if CACHED_AWS_CLIENT is None:
        CACHED_AWS_CLIENT = get_cached_aws_client()
    target_role = f'arn:aws:iam::{account}:role/{AWS_AUDIT_ROLE_NAME}'
    try:
        dest_role = CACHED_AWS_CLIENT.assume_role(
            RoleArn=target_role,
            RoleSessionName=os.environ['EC2_INSTANCE_LIST_AWS_AUDIT_DESTINATION_ROLE_SESSION_NAME'],
            ExternalId=os.environ['AWS_AUDIT_DESTINATION_ROLE_ARN_EXTERNALID']
        )
    except Exception as e:
        return None
    ec2_session = boto3.Session(
        aws_access_key_id=dest_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=dest_role['Credentials']['SecretAccessKey'],
        aws_session_token=dest_role['Credentials']['SessionToken']
    )
    return ec2_session


GET_ACCOUNTS_QUERY = f"""
SELECT account:Id::string
FROM {AWS_ACCOUNTS_TABLE}
WHERE timestamp = (
  SELECT MAX(timestamp) FROM {AWS_ACCOUNTS_TABLE}
);
"""


def get_accounts_list(sf_client):
    accounts_list = []

    res = sf_client.cursor().execute(GET_ACCOUNTS_QUERY).fetchall()
    for account in res:
        accounts_list.append(account[0])
    return accounts_list


LOAD_INSTANCE_LIST_QUERY = f"""
INSERT INTO {INSTANCES_TABLE} (timestamp, instance)
SELECT '{{snapshotclock}}'::timestamp_ltz, PARSE_JSON(column1)
FROM VALUES {{format_string}}
"""


def get_data_worker(account):
    ec2_session = get_aws_client(account)
    if ec2_session:
        try:
            ec2_regions = [region['RegionName'] for region in ec2_session.client('ec2').describe_regions()['Regions']]
        except Exception as e:
            return None
        for region in ec2_regions:
            try:
                client = ec2_session.client('ec2', region_name=region)
                paginator = client.get_paginator('describe_instances')
                page_iterator = paginator.paginate()
                instances = [instance for page in page_iterator for instance_array in page['Reservations'] for instance in instance_array['Instances']]
            except Exception as e:
                return None
        instance_list = [json.dumps({**instance,"AccountId":account}, default=str) for instance in instances]
        return instance_list

def get_data(accounts_list):
    start = datetime.datetime.now()
    instance_list_list = Pool(4).map(get_data_worker, accounts_list)
    instance_list = [x for l in instance_list_list if l for x in l]
    if instance_list:
        sf_client = get_snowflake_client()
        instance_groups = groups_of(15000,instance_list)
        for group in instance_groups:
            query = LOAD_INSTANCE_LIST_QUERY.format(
                snapshotclock=datetime.datetime.utcnow().isoformat(),
                format_string=", ".join(["(%s)"] * len(group)))
            sf_client.cursor().execute(query, group)
    end = datetime.datetime.now()
    print(f"start: {start} end: {end} total: {(end - start).total_seconds()}")

def main():
    sf_client = get_snowflake_client()
    current_time = datetime.datetime.now(datetime.timezone.utc)
    last_time = sf_client.cursor().execute(f'SELECT max(timestamp) FROM {INSTANCES_TABLE}').fetchall()[0][0]
    if last_time is None or (current_time - last_time).total_seconds() > 86400:
        accounts_list = get_accounts_list(sf_client)
        get_data(accounts_list)
    else:
        print("It's not time yet!")


if __name__ == "__main__":
    main()
