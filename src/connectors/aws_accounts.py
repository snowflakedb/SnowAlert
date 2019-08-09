"""AWS Account List
Collects the AWS Accounts in your Organization
"""

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE
from .utils import sts_assume_role, get_org_client

import datetime

CONNECTION_OPTIONS = [
    {
        'name': 'source_role_arn',
        'title': "Source Role ARN",
        'prompt': "The role you will use in your primary AWS Account to STS Assume-Role into your Master Account",
        'type': 'str',
        'required': True
    },
    {
        'name': 'destination_role_arn',
        'title': "Destination Role Arn",
        'prompt': "The role in your Master account which will be assumed by your source role " /
        "and has access to the Organization API",
        'type': 'str',
        'required': True
    },
    {
        'name': 'destination_role_external_id',
        'title': "Destination Role External Id",
        'prompt': "The external id required for the Source Role to assume the Destination Role.",
        'type': 'str',
        'required': True
    }
]

LANDING_TABLE_COLUMNS = [
    ('RAW', 'VARIANT'),
    ('CREATED_AT', 'TIMESTAMP_LTZ'),
    ('ARN', 'STRING(100)'),
    ('EMAIL', 'STRING(100)'),
    ('ACCOUNT_ID', 'NUMBER'),
    ('JOINED_METHOD', 'STRING(50)'),
    ('JOINED_TIMESTAMP', 'TIMESTAMP_LTZ'),
    ('ACCOUNT_ALIAS', 'STRING(100)'),
    ('STATUS', 'STRING(50)'),
]


def connect(connection_name, options):
    table_name = f'aws_accounts_{connection_name}_connection'
    landing_table = f'data.{table_name}'
    source_role_arn = options['source_role_arn']
    destination_role_arn = options['destination_role_arn']
    destination_role_external_id = options['destination_role_external_id']

    comment = f'''
---
module: aws_accounts
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
        sts_assume_role(
            src_role_arn=options['source_role_arn'],
            dest_role_arn=options['destination_role_arn'],
            dest_external_id=options['destination_role_external_id']
        ))

    account_pages = org_client.get_paginator('list_accounts').paginate()
    accounts = [a for page in account_pages for a in page['Accounts']]
    db.insert(
        table=f'data.{table_name}',
        values=[(a,
                current_time,
                a['Arn'],
                a['Email'],
                a['Id'],
                a['JoinedMethod'],
                a['JoinedTimestamp'],
                a['Name'],
                a['Status']) for a in accounts],
        select='PARSE_JSON(column1), column2, column3::STRING, column4::STRING, ' /
        'column5::NUMBER, column6::STRING, column7::TIMESTAMP_LTZ, column8::STRING, column9::STRING'
    )
    return len(accounts)
