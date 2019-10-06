"""AWS Asset Inventory
Collect AWS EC2, SG, ELB, IAM assets using an Access Key or privileged Role
"""
from datetime import datetime
import json

import boto3

from runners.helpers import db, log
from runners.helpers.dbconfig import REGION, ROLE as SA_ROLE
from runners.config import RUN_ID
from .utils import create_metadata_table, sts_assume_role, yaml_dump

AWS_ACCOUNTS_METADATA = 'data.aws_accounts_information'

CONNECTION_OPTIONS = [
    {
        'type': 'select',
        'options': [
            {'value': 'EC2', 'label': "EC2 Inventory"},
            {'value': 'SG', 'label': "SG Inventory"},
            {'value': 'ELB', 'label': "ELB Inventory"},
            {'value': 'IAM', 'label': "IAM Inventory"},
        ],
        'name': 'connection_type',
        'title': "Asset Type",
        'prompt': "The type of AWS asset information you are ingesting to Snowflake.",
        'required': True,
    },
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
    },
]

LANDING_TABLES_COLUMNS = {
    # EC2 Instances Landing Table
    'EC2': [
        ('raw', 'VARIANT'),
        ('instance_id', 'STRING(30)'),
        ('architecture', 'STRING(16)'),
        ('monitored_time_utc', 'TIMESTAMP_TZ'),
        ('instance_type', 'STRING(256)'),
        ('key_name', 'STRING(256)'),
        ('launch_time', 'TIMESTAMP_TZ'),
        ('region_name', 'STRING(16)'),
        ('instance_state', 'STRING(16)'),
        ('instance_name', 'STRING(256)'),
        ('account_id', 'STRING(30)'),
    ],
    # Security Group Landing table
    'SG': [
        ('raw', 'VARIANT'),
        ('description', 'STRING(256)'),
        ('monitored_time', 'TIMESTAMP_TZ'),
        ('group_id', 'STRING(30)'),
        ('group_name', 'STRING(255)'),
        ('account_id', 'STRING(30)'),
        ('region_name', 'STRING(16)'),
        ('vpc_id', 'STRING(30)'),
    ],
    # Elastic Load Balancer landing table
    'ELB': [
        ('raw', 'VARIANT'),
        ('monitored_time', 'TIMESTAMP_TZ'),
        ('hosted_zone_name', 'STRING(256)'),
        ('hosted_zone_name_id', 'STRING(30)'),
        ('created_time', 'TIMESTAMP_TZ'),
        ('dns_name', 'STRING(512)'),
        ('load_balancer_name', 'STRING(256)'),
        ('region_name', 'STRING(16)'),
        ('scheme', 'STRING(30)'),
        ('vpc_id', 'STRING(30)'),
        ('account_id', 'STRING(30)'),
    ],
    # IAM Users
    'IAM': [
        ('raw', 'VARCHAR'),
        ('ingested_at', 'TIMESTAMP_LTZ'),
        ('path', 'VARCHAR'),
        ('user_name', 'VARCHAR'),
        ('user_id', 'VARCHAR'),
        ('arn', 'VARCHAR'),
        ('create_date', 'TIMESTAMP_LTZ'),
        ('password_last_used', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING(32)'),
    ],
}


def connect(connection_name, options):
    connection_type = options['connection_type']
    columns = LANDING_TABLES_COLUMNS[connection_type]

    msg = create_asset_table(connection_name, connection_type, columns, options)

    return {'newStage': 'finalized', 'newMessage': msg}


def create_asset_table(connection_name, asset_type, columns, options):
    # create the tables, based on the config type (i.e. SG, EC2, ELB)
    table_name = f'aws_asset_inv_{asset_type}_{connection_name}_connection'
    landing_table = f'data.{table_name}'

    comment = yaml_dump(module='aws_inventory', **options)

    db.create_table(name=landing_table, cols=columns, comment=comment)
    metadata_cols = [
        ('snapshot_at', 'TIMESTAMP_LTZ'),
        ('run_id', 'VARCHAR(100)'),
        ('account_id', 'VARCHAR(100)'),
        ('account_alias', 'VARCHAR(100)'),
        (f'{asset_type}_count', 'NUMBER'),
        ('error', 'VARCHAR'),
    ]
    create_metadata_table(
        table=AWS_ACCOUNTS_METADATA, cols=metadata_cols, addition=metadata_cols[4]
    )
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')

    return f"AWS {asset_type} asset ingestion table created!"


def ingest(table_name, options: dict):
    landing_table = f'data.{table_name}'
    connection_type = options['connection_type']

    aws_access_key = options.get('aws_access_key')
    aws_secret_key = options.get('aws_secret_key')

    source_role_arn = options.get('source_role_arn')
    destination_role_name = options.get('destination_role_name')
    external_id = options.get('external_id')
    accounts_connection_name = options.get('accounts_connection_name', '')

    if not accounts_connection_name.startswith('data.'):
        accounts_connection_name = 'data.' + accounts_connection_name

    ingest_of_type = {
        'EC2': ec2_dispatch,
        'SG': sg_dispatch,
        'ELB': elb_dispatch,
        'IAM': iam_dispatch,
    }[connection_type]

    if (
        source_role_arn
        and destination_role_name
        and external_id
        and accounts_connection_name
    ):
        # get accounts list, pass list into ingest ec2
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
            external_id=external_id,
        )

    elif aws_access_key and aws_secret_key:
        count = ingest_of_type(
            landing_table, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key
        )
        log.info(f'Inserted {count} rows.')
        yield count
    else:
        log.error()


def iam_dispatch(
    landing_table,
    aws_access_key='',
    aws_secret_key='',
    accounts=None,
    source_role_arn='',
    destination_role_name='',
    external_id='',
):
    results = 0
    if accounts:
        for account in accounts:
            id = account['ACCOUNT_ID']
            name = account['ACCOUNT_ALIAS']
            target_role = f'arn:aws:iam::{id}:role/{destination_role_name}'
            log.info(f"Using role {target_role}")
            try:
                session = sts_assume_role(source_role_arn, target_role, external_id)

                results += ingest_iam(landing_table, session=session, account=account)

                db.insert(
                    AWS_ACCOUNTS_METADATA,
                    values=[(datetime.utcnow(), RUN_ID, id, name, results)],
                    columns=[
                        'snapshot_at',
                        'run_id',
                        'account_id',
                        'account_alias',
                        'iam_count',
                    ],
                )

            except Exception as e:
                db.insert(
                    AWS_ACCOUNTS_METADATA,
                    values=[(datetime.utcnow(), RUN_ID, id, name, 0, e)],
                    columns=[
                        'snapshot_at',
                        'run_id',
                        'account_id',
                        'account_alias',
                        'iam_count',
                        'error',
                    ],
                )
                log.error(f"Unable to assume role {target_role} with error", e)
    else:
        results += ingest_iam(
            landing_table, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key
        )

    return results


def ec2_dispatch(
    landing_table,
    aws_access_key='',
    aws_secret_key='',
    accounts=None,
    source_role_arn='',
    destination_role_name='',
    external_id='',
):
    results = 0
    if accounts:
        for account in accounts:
            id = account['ACCOUNT_ID']
            name = account['ACCOUNT_ALIAS']
            target_role = f'arn:aws:iam::{id}:role/{destination_role_name}'
            log.info(f"Using role {target_role}")
            try:
                session = sts_assume_role(source_role_arn, target_role, external_id)

                results += ingest_ec2(landing_table, session=session, account=account)

                db.insert(
                    AWS_ACCOUNTS_METADATA,
                    values=[(datetime.utcnow(), RUN_ID, id, name, results)],
                    columns=[
                        'snapshot_at',
                        'run_id',
                        'account_id',
                        'account_alias',
                        'ec2_count',
                    ],
                )

            except Exception as e:
                db.insert(
                    AWS_ACCOUNTS_METADATA,
                    values=[(datetime.utcnow(), RUN_ID, id, name, 0, e)],
                    columns=[
                        'snapshot_at',
                        'run_id',
                        'account_id',
                        'account_alias',
                        'ec2_count',
                        'error',
                    ],
                )
                log.error(f"Unable to assume role {target_role} with error", e)
    else:
        results += ingest_ec2(
            landing_table, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key
        )

    return results


def sg_dispatch(
    landing_table,
    aws_access_key='',
    aws_secret_key='',
    accounts=None,
    source_role_arn='',
    destination_role_name='',
    external_id='',
):
    results = 0
    if accounts:
        for account in accounts:
            id = account['ACCOUNT_ID']
            name = account['ACCOUNT_ALIAS']
            target_role = f'arn:aws:iam::{id}:role/{destination_role_name}'
            log.info(f"Using role {target_role}")
            try:
                session = sts_assume_role(source_role_arn, target_role, external_id)
                results += ingest_sg(landing_table, session=session, account=account)

                db.insert(
                    AWS_ACCOUNTS_METADATA,
                    values=[(datetime.utcnow(), RUN_ID, id, name, results)],
                    columns=[
                        'snapshot_at',
                        'run_id',
                        'account_id',
                        'account_alias',
                        'sg_count',
                    ],
                )
            except Exception as e:
                db.insert(
                    AWS_ACCOUNTS_METADATA,
                    values=[(datetime.utcnow(), RUN_ID, id, name, 0, e)],
                    columns=[
                        'snapshot_at',
                        'run_id',
                        'account_id',
                        'account_alias',
                        'sg_count',
                        'error',
                    ],
                )
                log.error(f"Unable to assume role {target_role} with error", e)
    else:
        results += ingest_sg(
            landing_table, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key
        )

    return results


def elb_dispatch(
    landing_table,
    aws_access_key='',
    aws_secret_key='',
    accounts=None,
    source_role_arn='',
    destination_role_name='',
    external_id='',
):
    results = 0
    if accounts:
        for account in accounts:
            id = account['ACCOUNT_ID']
            name = account['ACCOUNT_ALIAS']
            target_role = f'arn:aws:iam::{id}:role/{destination_role_name}'
            log.info(f"Using role {target_role}")
            try:
                session = sts_assume_role(source_role_arn, target_role, external_id)
                results += ingest_elb(landing_table, session=session, account=account)

                db.insert(
                    AWS_ACCOUNTS_METADATA,
                    values=[(datetime.utcnow(), RUN_ID, id, name, results)],
                    columns=[
                        'snapshot_at',
                        'run_id',
                        'account_id',
                        'account_alias',
                        'elb_count',
                    ],
                )
            except Exception as e:
                db.insert(
                    AWS_ACCOUNTS_METADATA,
                    values=[(datetime.utcnow(), RUN_ID, id, name, 0, e)],
                    columns=[
                        'snapshot_at',
                        'run_id',
                        'account_id',
                        'account_alias',
                        'elb_count',
                        'error',
                    ],
                )
                log.error(f"Unable to assume role {target_role} with error", e)
    else:
        results += ingest_elb(
            landing_table, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key
        )

    return results


def ingest_iam(
    landing_table, aws_access_key=None, aws_secret_key=None, session=None, account=None
):
    users = get_iam_users(
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        session=session,
        account=account,
    )

    monitor_time = datetime.utcnow().isoformat()

    db.insert(
        landing_table,
        values=[
            (
                row,
                monitor_time,
                row['Path'],
                row['UserName'],
                row['UserId'],
                row.get('Arn'),
                row['CreateDate'],
                row.get('PasswordLastUsed'),
                row.get('Account', {}).get('ACCOUNT_ID'),
            )
            for row in users
        ],
        select=db.derive_insert_select(LANDING_TABLES_COLUMNS['IAM']),
        columns=db.derive_insert_columns(LANDING_TABLES_COLUMNS['IAM']),
    )

    return len(users)


def ingest_ec2(
    landing_table, aws_access_key=None, aws_secret_key=None, session=None, account=None
):
    instances = get_ec2_instances(
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        session=session,
        account=account,
    )

    monitor_time = datetime.utcnow().isoformat()
    db.insert(
        landing_table,
        values=[
            (
                row,
                row['InstanceId'],
                row['Architecture'],
                monitor_time,
                row['InstanceType'],
                # can be not present if a managed instance such as EMR
                row.get('KeyName', ''),
                row['LaunchTime'],
                row['Region']['RegionName'],
                row['State']['Name'],
                row.get('InstanceName', ''),
                row.get('Account', {}).get('ACCOUNT_ID'),
            )
            for row in instances
        ],
        select='PARSE_JSON(column1), column2, column3, column4, column5, column6, column7, column8, column9, column10',
    )

    return len(instances)


def ingest_sg(
    landing_table, aws_access_key=None, aws_secret_key=None, session=None, account=None
):
    groups = get_all_security_groups(
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        session=session,
        account=account,
    )
    monitor_time = datetime.utcnow().isoformat()
    db.insert(
        landing_table,
        values=[
            (
                row,
                row['Description'],
                monitor_time,
                row['GroupId'],
                row['GroupName'],
                row['OwnerId'],
                row['Region']['RegionName'],
                row.get('VpcId'),
            )
            for row in groups
        ],
        select='PARSE_JSON(column1), column2, column3, column4, column5, column6, column7, column8',
    )
    return len(groups)


def ingest_elb(
    landing_table, aws_access_key=None, aws_secret_key=None, session=None, account=None
):
    elbs = get_all_elbs(
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        session=session,
        account=account,
    )
    monitor_time = datetime.utcnow().isoformat()

    db.insert(
        landing_table,
        values=[
            (
                row,
                monitor_time,
                row.get('CanonicalHostedZoneName', ''),
                row.get('CanonicalHostedZoneNameID', ''),
                row['CreatedTime'],
                row['DNSName'],
                row['LoadBalancerName'],
                row['Region']['RegionName'],
                row['Scheme'],
                row.get('VPCId', 'VpcId'),
                row.get('Account', {}).get('ACCOUNT_ID'),
            )
            for row in elbs
        ],
        select='PARSE_JSON(column1), column2, column3, column4, column5, column6, '
        'column7, column8, column9, column10',
    )
    return len(elbs)


def get_iam_users(aws_access_key=None, aws_secret_key=None, session=None, account=None):
    log.info(f"Searching for iam users.")

    # get list of all users
    if session:
        client = session.client('iam', region_name=REGION)
    else:
        client = boto3.client(
            'iam',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=REGION,
        )
    paginator = client.get_paginator('list_users')
    page_iterator = paginator.paginate()
    results = [user for page in page_iterator for user in page['Users']]

    for user in results:
        if account:
            user['Account'] = account

    # return list of users
    return results


def get_ec2_instances(
    aws_access_key=None, aws_secret_key=None, session=None, account=None
):
    client = boto3.client(
        'ec2',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=REGION,
    )
    regions = client.describe_regions()['Regions']

    log.info(f"Searching for EC2 instances in {len(regions)} region(s).")

    # get list of all instances in each region
    instances = []
    for region in regions:
        if session:
            client = session.client('ec2', region_name=region['RegionName'])
        else:
            client = boto3.client(
                'ec2',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region['RegionName'],
            )
        paginator = client.get_paginator('describe_instances')
        page_iterator = paginator.paginate()
        results = [
            instance
            for page in page_iterator
            for instance_array in page['Reservations']
            for instance in instance_array['Instances']
        ]
        for instance in results:
            instance['Region'] = region
            instance['Name'] = get_ec2_instance_name(instance)
            if account:
                instance['Account'] = account
        instances.extend(results)

    # return list of instances
    log.info(f"Successfully serialized {len(instances)} EC2 instance(s).")
    return instances


def get_ec2_instance_name(instance=None):
    """
    This method searches an ec2 instance object
    for the Name tag and returns that value as a string.
    """
    # return the name if possible, return empty string if not possible
    if "Tags" not in instance:
        return ""
    for tag in instance["Tags"]:
        if "Name" == tag["Key"]:
            return tag["Value"]


def get_all_security_groups(
    aws_access_key=None, aws_secret_key=None, session=None, account=None
):
    """
    This function grabs each security group from each region and returns
    a list of the security groups.

    Each security group is manually given a 'Region' field for clarity
    """

    regions = boto3.client(
        'ec2', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    ).describe_regions()['Regions']

    log.info(f"Searching for Security Groups in {len(regions)} region(s).")

    # get list of all groups in each region
    security_groups = []
    for region in regions:
        if session:
            ec2 = session.client('ec2', region_name=region['RegionName'])
        else:
            ec2 = boto3.client(
                'ec2',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region['RegionName'],
            )
        for group in ec2.describe_security_groups()['SecurityGroups']:
            group["Region"] = region
            if account:
                group["Account"] = account
            group_str = json.dumps(group, default=datetime_serializer).encode(
                "utf-8"
            )  # for the boto3 datetime fix
            group = json.loads(group_str)
            security_groups.append(group)

    # return list of groups
    log.info(f"Successfully serialized {len(security_groups)} security group(s).")
    return security_groups


def get_all_elbs(aws_access_key=None, aws_secret_key=None, session=None, account=None):
    v1_elbs = get_all_v1_elbs(
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        session=session,
        account=account,
    )
    v2_elbs = get_all_v2_elbs(
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        session=session,
        account=account,
    )
    elbs = v1_elbs + v2_elbs

    if len(elbs) is 0:
        log.info("no elastic load balancers found")
        return []

    return elbs


def get_all_v1_elbs(
    aws_access_key=None, aws_secret_key=None, session=None, account=None
):
    """
    This function grabs each classic elb from each region and returns
    a list of them.
    """
    regions = boto3.client(
        'ec2', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    ).describe_regions()['Regions']

    log.info(f"Searching {len(regions)} region(s) for classic load balancers.")

    # get list of all load balancers in each region
    elbs = []
    for region in regions:
        if session:
            elb_client = session.client('elb', region_name=region['RegionName'])
        else:
            elb_client = boto3.client(
                'elb',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region['RegionName'],
            )
        for elb in elb_client.describe_load_balancers()['LoadBalancerDescriptions']:
            # add region before adding elb to list of elbs
            elb["Region"] = region
            if account:
                elb["Account"] = account
            elb_str = json.dumps(elb, default=datetime_serializer).encode(
                "utf-8"
            )  # for the datetime ser fix
            elb = json.loads(elb_str)
            elbs.append(elb)

    # return list of load balancers
    log.info(f"Successfully serialized {len(elbs)} classic elastic load balancers(s).")
    return elbs


def get_all_v2_elbs(
    aws_access_key=None, aws_secret_key=None, session=None, account=None
):
    """
    This function grabs each v2 elb from each region and returns
    a list of them.
    """
    regions = boto3.client(
        'ec2', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    ).describe_regions()['Regions']

    log.info(f"Searching {len(regions)} region(s) for modern load balancers.")

    # get list of all load balancers in each region
    elbs = []
    for region in regions:
        if session:
            elb_client = session.client('elbv2', region_name=region['RegionName'])
        else:
            elb_client = boto3.client(
                'elbv2',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region['RegionName'],
            )
        for elb in elb_client.describe_load_balancers()['LoadBalancers']:
            # add region
            elb["Region"] = region
            if account:
                elb["Account"] = account

            # add listeners to see which SSL policies are attached to this elb
            elb_arn = elb['LoadBalancerArn']
            listeners = elb_client.describe_listeners(LoadBalancerArn=elb_arn)
            elb["Listeners"] = listeners  # add listeners as field in the ELB
            elb = json.dumps(elb, default=datetime_serializer).encode("utf-8")
            elb = json.loads(elb)
            elbs.append(elb)

    # return list of load balancers
    log.info(f"Successfully serialized {len(elbs)} modern elastic load balancers(s).")
    return elbs


def datetime_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")
