"""AWS Account List
Collects the AWS Accounts in your Organization
"""

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE
from .utils import sts_assume_role

import datetime

CONNECTION_OPTIONS = [
    {
        'name': 'source_role_arn',
        'title': "Source Role ARN",
        'prompt': "Assumer Role ARN",
        'type': 'str',
        'required': True
    },
    {
        'name': 'destination_role_arn',
        'title': "Destination Role ARN",
        'prompt': "The destination role in your AWS Master Account",
        'type': 'str',
        'required': True
    },
    {
        'name': 'destination_role_external_id',
        'title': "Destination Role External Id",
        'prompt': "The external id required to assume the destination role.",
        'type': 'str',
        'required': True
    }
]

LANDING_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ'),
]


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
    org = sts_assume_role(
        src_role_arn=options['source_role_arn'],
        dest_role_arn=options['destination_role_arn'],
        dest_external_id=options['destination_role_external_id']
    ).client['organizations']
    account_pages = org.get_paginator('list_accounts').paginate()
    accounts = [a for page in account_pages for a in page['Accounts']]
    db.insert(
        table=f'data.{table_name}',
        values=[(a, current_time) for a in accounts],
        select='PARSE_JSON(column1), column2'
    )
    return len(accounts)
