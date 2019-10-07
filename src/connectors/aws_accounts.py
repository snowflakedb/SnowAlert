"""AWS Account List
Collects the AWS Accounts in your Organization
"""

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE

from .utils import sts_assume_role, yaml_dump

import datetime

CONNECTION_OPTIONS = [
    {
        'name': 'source_role_arn',
        'title': "Source Role ARN",
        'prompt': "The Role used in primary AWS Account to STS AssumeRole into Master Account",
        'type': 'str',
        'required': True,
    },
    {
        'name': 'destination_role_arn',
        'title': "Destination Role Arn",
        'prompt': "The Role in your Master account to be assumed by Source Role"
        "and has access to the Organization API",
        'type': 'str',
        'required': True,
    },
    {
        'name': 'destination_role_external_id',
        'title': "Destination Role External ID",
        'prompt': "The External ID required for Source Role to assume Destination Role.",
        'type': 'str',
        'required': True,
        'secret': True,
    },
]

LANDING_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('created_at', 'TIMESTAMP_LTZ'),
    ('arn', 'STRING(100)'),
    ('email', 'STRING(100)'),
    ('account_id', 'STRING(25)'),
    ('joined_method', 'STRING(50)'),
    ('joined_timestamp', 'TIMESTAMP_LTZ'),
    ('account_alias', 'STRING(100)'),
    ('status', 'STRING(50)'),
]


def connect(connection_name, options):
    table_name = f'aws_accounts_{connection_name}_connection'
    landing_table = f'data.{table_name}'
    source_role_arn = options['source_role_arn']
    destination_role_arn = options['destination_role_arn']
    destination_role_external_id = options['destination_role_external_id']

    comment = yaml_dump(
        module='aws_accounts',
        source_role_arn=source_role_arn,
        destination_role_arn=destination_role_arn,
        destination_role_external_id=destination_role_external_id,
    )

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')
    return {
        'newStage': 'finalized',
        'newMessage': "AWS Account ingestion table created!",
    }


def ingest(table_name, options):
    current_time = datetime.datetime.utcnow()
    org_client = sts_assume_role(
        src_role_arn=options['source_role_arn'],
        dest_role_arn=options['destination_role_arn'],
        dest_external_id=options['destination_role_external_id'],
    ).client('organizations')

    account_pages = org_client.get_paginator('list_accounts').paginate()
    accounts = [a for page in account_pages for a in page['Accounts']]
    db.insert(
        table=f'data.{table_name}',
        values=[
            (
                a,
                current_time,
                a['Arn'],
                a['Email'],
                a['Id'],
                a['JoinedMethod'],
                a['JoinedTimestamp'],
                a['Name'],
                a['Status'],
            )
            for a in accounts
        ],
        select=(
            'PARSE_JSON(column1)',
            'column2',
            'column3::STRING',
            'column4::STRING',
            'column5::STRING',
            'column6::STRING',
            'column7::TIMESTAMP_LTZ',
            'column8::STRING',
            'column9::STRING',
        ),
    )
    return len(accounts)
