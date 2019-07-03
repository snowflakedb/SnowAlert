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


def get_org_client(session_name, src_role_arn, dest_role_arn, dest_external_id):
    src_role = boto3.client('sts').assume_role(
        RoleArn=src_role_arn,
        RoleSessionName=session_name
    )
    dest_role = boto3.Session(
        aws_access_key_id=src_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=src_role['Credentials']['SecretAccessKey'],
        aws_session_token=src_role['Credentials']['SessionToken']
    ).client('sts').assume_role(
        RoleArn=dest_role_arn,
        RoleSessionName=session_name,
        ExternalId=dest_external_id
    )
    return boto3.Session(
        aws_access_key_id=dest_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=dest_role['Credentials']['SecretAccessKey'],
        aws_session_token=dest_role['Credentials']['SessionToken']
    ).client('organizations')


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
        'newMessage': "AWS Account ingestion table created!",
    }


def ingest(table_name, options):
    current_time = datetime.datetime.utcnow()
    org_client = get_org_client(
        session_name=table_name,
        src_role_arn=options['source_role_arn'],
        dest_role_arn=options['destination_role_arn'],
        dest_external_id=options['destination_role_external_id'],
    )
    account_pages = org_client.get_paginator('list_accounts').paginate()
    db.insert(
        table=f'data.{table_name}',
        values=[
            (a, current_time) for page in account_pages for a in page['Accounts']
        ],
        select='PARSE_JSON(column1), column2'
    )
