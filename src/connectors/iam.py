"""AWS Asset Inventory
Collect AWS IAM assets using an Access Key or privileged Role
"""
from datetime import datetime
import json

import boto3

from runners.helpers import db, log
from runners.helpers.dbconfig import REGION, ROLE as SA_ROLE
from runners.config import RUN_ID
#from .utils import create_metadata_table, sts_assume_role, yaml_dump
#from .utils import create_metadata_table, sts_assume_role

AWS_IAM_METADATA = 'data.aws_iam_information'

CONNECTION_OPTIONS = [
    {
        # The AWS Client ID. The account ID is not necessary as Client ID's are globally unique
        'name': 'aws_access_key',
        'title': "AWS Access Key",
        'prompt': (
            "If provided, this key id will be used to authenticate to a single AWS Account. You must provide either "
            "an access key and secret key pair, or a source role, destination role, external id, and accounts "
            "connection identifier."
        ),
        'type': 'str',
        'placeholder': 'AKIAQWERTYUIOPASDFGH (NEEDED WITH SECRET KEY)',
    },
    {
        # The AWS Secret Key
        'type': 'str',
        'name': 'aws_secret_key',
        'title': "AWS Secret Key",
        'prompt': (
            "If provided, this secret key will be used to authenticate to a single AWS Account. You must provide "
            "either an access key and secret key pair, or a source role, destination role, external id, and "
            "accounts connection identifier."
        ),
        'secret': True,
        'placeholder': 'WGndo5/Flssn3FnsOIuYwiei9NbsemsNLK96sdSF (NEEDED WITH ACCESS KEY)',
    },
    {
        'type': 'str',
        'name': 'source_role_arn',
        'title': "Source Role ARN",
        'prompt': (
            "If provided, this role will be used to STS AssumeRole into accounts from the AWS Accounts Connection "
            "Table. You must provide either an access key and secret key pair, or a source role, destination role, "
            "external id, and accounts connection identifier."
        ),
        'placeholder': (
            "arn:aws:iam::1234567890987:role/sample-audit-assumer "
            "(NEEDED WITH DESTINATION ROLE NAME, EXTERNAL ID, AND ACCOUNTS CONNECTION IDENTIFIER)"
        ),
    },
    {
        'type': 'str',
        'name': 'destination_role_name',
        'title': "Destination Role Name",
        'prompt': (
            "If provided, this role is the target destination role in each account listed by the AWS Accounts "
            "Connector. You must provide either an access key and secret key pair, or a source role, destination "
            "role, external id, and accounts connection identifier. and has access to the Organization API"
        ),
        'placeholder': (
            "sample-audit-role "
            "(NEEDED WITH SOURCE ROLE ARN, EXTERNAL ID, AND ACCOUNTS CONNECTION IDENTIFIER)"
        ),
    },
    {
        'type': 'str',
        'name': 'external_id',
        'title': "Destination Role External ID",
        'prompt': (
            "The External ID required for Source Role to assume Destination Role. You must provide either an access "
            "key and secret key pair, or a source role, destination role, external id, and accounts connection "
            "identifier."
        ),
        'placeholder': (
            "sample_external_id "
            "(NEEDED WITH SOURCE ROLE ARN, DESTINATION ROLE NAME, AND ACCOUNTS CONNECTION IDENTIFIER)"
        ),
    },
    {
        'type': 'str',
        'name': 'accounts_connection_name',
        'title': "AWS Accounts Table Name",
        'prompt': (
            "The name for your AWS Accounts Connection. You must provide either an "
            "access key and secret key pair, or a source role, destination role, external id, and accounts "
            "connection table name."
        ),
        'placeholder': (
            "AWS_ACCOUNTS_DEFAULT_CONNECTION (NEEDED WITH SOURCE ROLE ARN, DESTINATION ROLE ARN, AND EXTERNAL ID)"
        ),
    }
]

LANDING_TABLES_COLUMNS = {
    ('raw', 'VARCHAR'),
    ('ingested_at', 'TIMESTAMP_LTZ'),
    ('Path', 'VARCHAR'),
    ('UserName', 'VARCHAR'),
    ('UserId', 'VARCHAR'),
    ('Arn', 'VARCHAR'),
    ('CreateDate', 'TIMESTAMP_LTZ'),
    ('PasswordLastUsed', 'TIMESTAMP_LTZ')
}


def connect(connection_name, options):
    columns = LANDING_TABLES_COLUMNS
    landing_table = f'data.iam_{connection_name}_connection'

    # comment = yaml_dump(
    #     module='iam',
    #     **options
    # )
    comment = ""

    db.create_table(name=landing_table, cols=columns, comment=comment)
    metadata_cols = [
        ('snapshot_at', 'TIMESTAMP_LTZ'),
        ('run_id', 'VARCHAR(100)'),
        ('account_id', 'VARCHAR(100)'),
        ('account_alias', 'VARCHAR(100)'),
        (f'iam_count', 'NUMBER'),
        ('error', 'VARCHAR')
    ]
    #create_metadata_table(table=AWS_IAM_METADATA, cols=metadata_cols, addition=metadata_cols[4])
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': 'IAM ingestion table was created!',
    }


def ingest(table_name, options):
    landing_table = f'data.{table_name}'
    aws_access_key = options.get('aws_access_key')
    aws_secret_key = options.get('aws_secret_key')
    connection_type = options.get('connection_type')
    source_role_arn = options.get('source_role_arn')
    destination_role_name = options.get('destination_role_name')
    external_id = options.get('external_id')
    accounts_connection_name = options.get('accounts_connection_name')

    if not accounts_connection_name.startswith('data.'):
        accounts_connection_name = 'data.' + accounts_connection_name

    ingest_of_type = {
        'IAM': iam_dispatch,
    }[connection_type]

    if source_role_arn and destination_role_name and external_id and accounts_connection_name:
        # get accounts list, pass list into ingest iam
        query = (
            f"SELECT account_id, account_alias "
            f"FROM {accounts_connection_name} "
            f"WHERE created_at = ("
            f"  SELECT MAX(created_at)"
            f"  FROM {accounts_connection_name}"
            f")"
        )
        accounts = db.fetch(query)
        count = ingest_of_type(
            landing_table,
            accounts=accounts,
            source_role_arn=source_role_arn,
            destination_role_name=destination_role_name,
            external_id=external_id
        )

    elif aws_access_key and aws_secret_key:
        count = ingest_of_type(landing_table, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)
        log.info(f'Inserted {count} rows.')
        yield count
    else:
        log.error()


def iam_dispatch(landing_table, aws_access_key='', aws_secret_key='', accounts=None, source_role_arn='',
                 destination_role_name='', external_id=''):
    results = 0
    if accounts:
        for account in accounts:
            id = account['ACCOUNT_ID']
            name = account['ACCOUNT_ALIAS']
            target_role = f'arn:aws:iam::{id}:role/{destination_role_name}'
            log.info(f"Using role {target_role}")
            try:
                #session = sts_assume_role(source_role_arn, target_role, external_id)

                results += ingest_iam(landing_table, session="session", account=account)

                db.insert(
                    AWS_IAM_METADATA,
                    values=[(datetime.utcnow(), RUN_ID, id, name, results)],
                    columns=['snapshot_at', 'run_id', 'account_id', 'account_alias', 'iam_count']
                )

            except Exception as e:
                db.insert(
                    AWS_IAM_METADATA,
                    values=[(datetime.utcnow(), RUN_ID, id, name, 0, e)],
                    columns=['snapshot_at', 'run_id', 'account_id', 'account_alias', 'iam_count', 'error']
                )
                log.error(f"Unable to assume role {target_role} with error", e)
    else:
        results += ingest_iam(landing_table, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)

    return results


def ingest_iam(landing_table, aws_access_key=None, aws_secret_key=None, session=None, account=None):
    users = get_iam_users(
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        session=session,
        account=account
    )

    monitor_time = datetime.utcnow().isoformat()

    for row in users:
        print(row)
        print(monitor_time)
        print(row.get('Path'))
        print(row.get('UserName'))
        print(row.get('UserId'))
        print(row.get('Arn'))
        print(row.get('CreateDate'))
        print(row.get('PasswordLastUsed'))

    db.insert(
        landing_table,
        values=[(
            row,
            monitor_time,
            row.get('Path'),
            row.get('UserName'),
            row.get('UserId'),
            row.get('Arn'),
            row.get('CreateDate'),
            row.get('PasswordLastUsed'))
            for row in users
        ],
        select='PARSE_JSON(column1), column2, column3, column4, column5, column6, column7, column8'
    )
    print("done with all")

    return len(users)


def get_iam_users(aws_access_key=None, aws_secret_key=None, session=None, account=None):
    client = boto3.client(
        'iam',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=REGION,
    )

    log.info(f"Searching for iam users.")

    # get list of all users
    if session:
        client = session.client('iam', region_name=REGION)
    else:
        client = boto3.client('iam', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                                region_name=REGION)
    paginator = client.get_paginator('list_users')
    page_iterator = paginator.paginate()
    results = [
        user
        for page in page_iterator
        for user in page['Users']
    ]

    # return list of users
    return results
