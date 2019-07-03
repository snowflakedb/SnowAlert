"""AWS Account List
Lists your AWS Accounts
"""

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE

import boto3
import datetime

CONNECTION_OPTIONS = [
    {
        'name': 'source_role_arn',
        'title': "source role arn",
        'prompt': "assumer role arn",
        'type': 'str',
        'required': True
    },
    {
        'name': 'destination_role_arn',
        'title': "destination role arn",
        'prompt': "destination role arn",
        'type': 'str',
        'required': True
    },
    {
        'name': 'destination_role_external_id',
        'title': 'destination_role_external_id',
        'prompt': 'destination_role_external_id',
        'type': 'str',
        'required': True
    }
]

LANDING_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ'),
]


def get_aws_client(table_name, options):
    sts_client_source = boto3.client('sts')
    sts_client_source_response = sts_client_source.assume_role(
        RoleArn=options['source_role_arn'],
        RoleSessionName=table_name
    )
    sts_client_destination = boto3.Session(
        aws_access_key_id=sts_client_source_response['Credentials']['AccessKeyId'],
        aws_secret_access_key=sts_client_source_response['Credentials']['SecretAccessKey'],
        aws_session_token=sts_client_source_response['Credentials']['SessionToken']
    )
    sts = sts_client_destination.client('sts')
    response = sts.assume_role(
        RoleArn=options['destination_role_arn'],
        RoleSessionName=table_name,
        ExternalId=options['destination_role_external_id']
    )
    org_session = boto3.Session(
        aws_access_key_id=response['Credentials']['AccessKeyId'],
        aws_secret_access_key=response['Credentials']['SecretAccessKey'],
        aws_session_token=response['Credentials']['SessionToken']
    )
    org_client = org_session.client('organizations')
    return org_client


def get_accounts_list(client):
    accounts = []
    paginator = client.get_paginator('list_accounts')
    page_iterator = paginator.paginate()
    for page in page_iterator:
        accounts.extend(page['Accounts'])

    accounts_list = [account for account in accounts]
    return accounts_list


def connect(connection_name, options):
    table_name = f'aws_account_{connection_name}_connection'
    landing_table = f'data.{table_name}'
    source_role_arn = options['source_role_arn']
    destination_role_arn = options['destination_role_arn']
    destination_role_external_id = options['destination_role_external_id']

    comment = f'''
---
module: aws_account
source_role_arn: {source_role_arn}
destination_role_arn: {destination_role_arn}
destination_role_external_id: {destination_role_external_id}
'''

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')
    return {
        'newStage': 'finalized',
        'newMessage': "AWS_Account ingestion table created!",
    }


def ingest(table_name, options):
    current_time = datetime.datetime.utcnow()
    account_list = get_accounts_list(get_aws_client(table_name, options))
    db.insert(table=f'data.{table_name}', values=[
                    (account,
                     current_time,
                     )
              for account in account_list
              ],
              select='PARSE_JSON(column1), column2')
