import boto3
import base64
from datetime import datetime, timezone
import json
import os

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from multiprocessing import Pool
from runners.utils import groups_of
from runners.helpers import db, log

import snowflake.connector

INSTANCES_TABLE = os.environ['EC2_INSTANCE_LIST_SNOWFLAKE_TABLE_IDENTIFIER']
AWS_ACCOUNTS_TABLE = os.environ['LIST_ACCOUNTS_SNOWFLAKE_TABLE_IDENTIFIER']
AWS_ACCOUNTS_INFORMATION_TABLE = os.environ['AWS_ACCOUNTS_INFORMATION_TABLE_IDENTIFIER']
AWS_AUDIT_ROLE_NAME = os.environ['AWS_AUDIT_DESTINATION_ROLE_NAME']
SA_ACCOUNT = os.environ['INGEST_SNOWFLAKE_ACCOUNT']
SA_USER = os.environ['INGEST_SNOWFLAKE_USER']
SA_USER_PK = os.environ['INGEST_SNOWFLAKE_USER_PRIVATE_KEY']
SA_USER_PK_PASSWORD = os.environ['INGEST_SNOWFLAKE_USER_PRIVATE_KEY_PASSWORD']
CACHED_AWS_CLIENT = None


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


def get_cached_aws_client():
    sts_client_source = boto3.client('sts')
    sts_client_source_response = sts_client_source.assume_role(
        RoleArn=os.environ['AWS_AUDIT_SOURCE_ROLE_ARN'],
        RoleSessionName=os.environ[
            'EC2_INSTANCE_LIST_AWS_AUDIT_SOURCE_ROLE_SESSION_NAME'
        ],
    )
    sts_client_destination = boto3.Session(
        aws_access_key_id=sts_client_source_response['Credentials']['AccessKeyId'],
        aws_secret_access_key=sts_client_source_response['Credentials'][
            'SecretAccessKey'
        ],
        aws_session_token=sts_client_source_response['Credentials']['SessionToken'],
    )
    sts_client = sts_client_destination.client('sts')
    return sts_client


def get_aws_client(account_id):
    global CACHED_AWS_CLIENT
    if CACHED_AWS_CLIENT is None:
        CACHED_AWS_CLIENT = get_cached_aws_client()
    target_role = f'arn:aws:iam::{account_id}:role/{AWS_AUDIT_ROLE_NAME}'
    dest_role = CACHED_AWS_CLIENT.assume_role(
        RoleArn=target_role,
        RoleSessionName=os.environ[
            'EC2_INSTANCE_LIST_AWS_AUDIT_DESTINATION_ROLE_SESSION_NAME'
        ],
        ExternalId=os.environ['AWS_AUDIT_DESTINATION_ROLE_ARN_EXTERNALID'],
    )
    ec2_session = boto3.Session(
        aws_access_key_id=dest_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=dest_role['Credentials']['SecretAccessKey'],
        aws_session_token=dest_role['Credentials']['SessionToken'],
    )
    return ec2_session


GET_ACCOUNTS_QUERY = f"""
SELECT account:Id::STRING, account:Name::STRING
FROM {AWS_ACCOUNTS_TABLE}
WHERE timestamp = (
  SELECT MAX(timestamp) FROM {AWS_ACCOUNTS_TABLE}
);
"""


def get_accounts_list(sf_client):
    res = sf_client.cursor().execute(GET_ACCOUNTS_QUERY).fetchall()
    return res


LOAD_INSTANCE_LIST_QUERY = f"""
INSERT INTO {INSTANCES_TABLE} (timestamp, instance)
SELECT '{{snapshotclock}}'::timestamp_ltz, PARSE_JSON(column1)
FROM VALUES {{format_string}}
"""


def get_data_worker(account_id, account_name):
    try:
        ec2_session = get_aws_client(account_id)
        instances = []
        try:
            ec2_regions = [
                region['RegionName']
                for region in ec2_session.client('ec2').describe_regions()['Regions']
            ]
        except Exception as e:
            log.info(
                f"ec2_describe_instances account [{account_id}] {account_name} exception",
                e,
            )
            return None
        for region in ec2_regions:
            try:
                client = ec2_session.client('ec2', region_name=region)
                paginator = client.get_paginator('describe_instances')
                page_iterator = paginator.paginate()
                region = [
                    instance
                    for page in page_iterator
                    for instance_array in page['Reservations']
                    for instance in instance_array['Instances']
                ]
                instances.extend(region)
            except Exception as e:
                log.info(
                    f"ec2_describe_instances: account [{account_id}] {account_name} exception",
                    e,
                )
                db.insert(
                    AWS_ACCOUNTS_INFORMATION_TABLE,
                    values=[(datetime.utcnow(), account_id, account_name, None, e)],
                )
                return None
        instance_list = [
            json.dumps({**instance, "AccountId": account_id}, default=str)
            for instance in instances
        ]
        try:
            db.insert(
                AWS_ACCOUNTS_INFORMATION_TABLE,
                values=[
                    (
                        datetime.utcnow(),
                        account_id,
                        account_name,
                        len(instance_list),
                        None,
                    )
                ],
            )
        except Exception:
            print('Failed to insert into AWS_ACCOUNT_INFORMATION table.')
        print(
            f"ec2_describe_instances: account: {account_name} instances: {len(instance_list)}"
        )
        return instance_list
    except Exception as e:
        print(f"ec2_describe_instances: account: {account_name} exception: {e}")
        db.insert(
            AWS_ACCOUNTS_INFORMATION_TABLE,
            values=[(datetime.utcnow(), account_id, account_name, None, e)],
        )
        return None


def get_data(accounts_list):
    start = datetime.now()
    instance_list_list = Pool(4).starmap(get_data_worker, accounts_list)
    instance_list = [x for l in instance_list_list if l for x in l]
    if instance_list:
        sf_client = get_snowflake_client()
        instance_groups = groups_of(15000, instance_list)
        snapshot_time = datetime.utcnow().isoformat()
        for group in instance_groups:
            query = LOAD_INSTANCE_LIST_QUERY.format(
                snapshotclock=snapshot_time,
                format_string=", ".join(["(%s)"] * len(group)),
            )
            sf_client.cursor().execute(query, group)
    end = datetime.now()
    print(f"start: {start} end: {end} total: {(end - start).total_seconds()}")


def main():
    sf_client = get_snowflake_client()
    current_time = datetime.now(timezone.utc)
    last_time = (
        sf_client.cursor()
        .execute(f'SELECT max(timestamp) FROM {INSTANCES_TABLE}')
        .fetchall()[0][0]
    )
    if (
        last_time is None or (current_time - last_time).total_seconds() > 3600
    ):  # 3600 seconds is one hour
        accounts_list = get_accounts_list(sf_client)
        get_data(accounts_list)
    else:
        print("ec2_describe_instances: It's not time yet!")


if __name__ == "__main__":
    main()
