import boto3
import base64
import datetime
import json
import os
import time

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from multiprocessing import Pool

import snowflake.connector

CREDENTIAL_REPORTS_TABLE = os.environ['IAM_CREDENTIAL_REPORTS_SNOWFLAKE_TABLE_IDENTIFIER']
PASSWORD_POLICY_TABLE = os.environ['IAM_CREDENTIAL_REPORTS_PASSWORD_POLICY_SNOWFLAKE_TABLE_IDENTIFIER']
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
        RoleSessionName=os.environ['IAM_CREDENTIAL_REPORTS_AWS_AUDIT_SOURCE_ROLE_SESSION_NAME']
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
            RoleSessionName=os.environ['IAM_CREDENTIAL_REPORTS_AWS_AUDIT_DESTINATION_ROLE_SESSION_NAME'],
            ExternalId=os.environ['AWS_AUDIT_DESTINATION_ROLE_ARN_EXTERNALID']
        )
    except Exception as e:
        return None
    iam_session = boto3.Session(
        aws_access_key_id=dest_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=dest_role['Credentials']['SecretAccessKey'],
        aws_session_token=dest_role['Credentials']['SessionToken']
    )
    return iam_session

def get_accounts_list(sf_client):
    accounts_list = []
    query = f"select account:Id::number from {AWS_ACCOUNTS_TABLE} where timestamp = (select max(timestamp) from {AWS_ACCOUNTS_TABLE});"
    res = sf_client.cursor().execute(query).fetchall()
    for account in res:
        accounts_list.append(account[0])
    return accounts_list


LOAD_REPORT_LIST_QUERY = f"""
INSERT INTO {CREDENTIAL_REPORTS_TABLE} (timestamp, report)
SELECT '{{snapshotclock}}'::timestamp_ltz, PARSE_JSON(column1)
FROM VALUES {{format_string}}
"""

LOAD_POLICY_LIST_QUERY = f"""
INSERT INTO {PASSWORD_POLICY_TABLE} (timestamp, policy)
SELECT '{{snapshotclock}}'::timestamp_ltz, PARSE_JSON(column1)
FROM VALUES {{format_string}}
"""

def get_data_worker(account):
    iam_session = get_aws_client(account)
    if iam_session:
        reports = []
        reports_json = []
        iam_client = iam_session.client('iam')
        iam_client.generate_credential_report()
        while(1):
            try:
                r = iam_client.get_credential_report()
                break
            except Exception as e:
                time.sleep(2)
        try:
            policy = iam_client.get_account_password_policy()['PasswordPolicy']
        except Exception as e:
            policy = {}
        report = r['Content'].decode('utf-8').split('\n')
        data = [i.split(',') for i in report]
        reports = [dict(zip(data[0], i)) for i in data[1:]]
        policy['AccountId'] = account
        reports_json = [json.dumps({**report, "account_id": account}) for report in reports]
        return {'report': reports_json, 'policy': json.dumps(policy)}

def get_data(accounts_list):
    start = datetime.datetime.now()
    results_list=list(filter(None,Pool(4).map(get_data_worker, accounts_list)))
    if results_list:
        policies_list = [result['policy'] for result in results_list]
        reports_list = [result['report'] for result in results_list]
        reports = []
        for report in reports_list:
            reports.extend(report)
        sf_client = get_snowflake_client()
        if len(reports):
            query = LOAD_REPORT_LIST_QUERY.format(
                snapshotclock=datetime.datetime.utcnow().isoformat(),
                format_string=", ".join(["(%s)"] * len(reports)))
            sf_client.cursor().execute(query, reports)
        if len(policies_list):
            query = LOAD_POLICY_LIST_QUERY.format(
                snapshotclock=datetime.datetime.utcnow().isoformat(),
                format_string=", ".join(["(%s)"] * len(policies_list)))
            sf_client.cursor().execute(query, policies_list)
    end = datetime.datetime.now()
    print(f"start: {start} end: {end} total: {(end - start).total_seconds()}")

def main():
    sf_client = get_snowflake_client()
    current_time = datetime.datetime.now(datetime.timezone.utc)
    last_time = sf_client.cursor().execute(f'SELECT max(timestamp) FROM {CREDENTIAL_REPORTS_TABLE}').fetchall()[0][0]
    if last_time is None or (current_time - last_time).total_seconds() > 86400:
        accounts_list = get_accounts_list(sf_client)
        get_data(accounts_list)
    else:
        print("It's not time yet!")

if __name__ == "__main__":
    main()
