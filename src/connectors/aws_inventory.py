"""AWS Asset Inventory
Collect AWS EC2, SG, ELB details using an Access Key
"""
from datetime import datetime
import json

import boto3

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE
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
        ],
        'name': 'connection_type',
        'title': "Asset Type",
        'prompt': "The type of AWS asset information you are ingesting to Snowflake.",
        'required': True
    },
    {
        # The AWS Client ID. The account ID is not necessary as Client ID's are globally unique
        'name': 'aws_access_key',
        'title': "AWS Access Key",
        'prompt': "If provided, this key id will be used to authenticate to a single AWS Account. You must provide either an access key and secret key pair, or a source role, destination role, external id, and accounts connection identifier.",
        'type': 'str',
    },
    {
        # The AWS Secret Key
        'name': 'aws_secret_key',
        'title': "AWS Secret Key",
        'prompt': "If provided, this secret key will be used to authenticate to a single AWS Account. You must provide either an access key and secret key pair, or a source role, destination role, external id, and accounts connection identifier.",
        'type': 'str',
        'secret': True,
    },
    {
        'name': 'source_role_arn',
        'title': "Source Role ARN",
        'prompt': "If provided, this role will be used to STS AssumeRole into accounts from the AWS Accounts Connection Table. You must provide either an access key and secret key pair, or a source role, destination role, external id, and accounts connection identifier.",
        'type': 'str',
    },
    {
        'name': 'destination_role_name',
        'title': "Destination Role Name",
        'prompt': "If provided, this role is the target destination role in each account listed by the AWS Accounts Connector. You must provide either an access key and secret key pair, or a source role, destination role, external id, and accounts connection identifier."
        "and has access to the Organization API",
        'type': 'str',
    },
    {
        'name': 'external_id',
        'title': "Destination Role External ID",
        'prompt': "The External ID required for Source Role to assume Destination Role. You must provide either an access key and secret key pair, or a source role, destination role, external id, and accounts connection identifier.",
        'type': 'str',
    },
    {
        'name': 'accounts_identifier',
        'title': "AWS Accounts Connection Identifier",
        'prompt': "The custom name for your AWS Accounts Connection, if you provided one. You must provide either an access key and secret key pair, or a source role, destination role, external id, and accounts connection identifier.",
        'type': 'str',
        'default': "default"
    }
]

LANDING_TABLES_COLUMNS = {
    # EC2 Instances Landing Table
    'EC2': [
        ('RAW', 'VARIANT'),
        ('INSTANCE_ID', 'STRING(30)'),
        ('ARCHITECTURE', 'STRING(16)'),
        ('MONITORED_TIME_UTC', 'TIMESTAMP_TZ'),
        ('INSTANCE_TYPE', 'STRING(256)'),
        ('KEY_NAME', 'STRING(256)'),
        ('LAUNCH_TIME', 'TIMESTAMP_TZ'),
        ('REGION_NAME', 'STRING(16)'),
        ('INSTANCE_STATE', 'STRING(16)'),
        ('INSTANCE_NAME', 'STRING(256)')
    ],
    # Security Group Landing table
    'SG': [
        ('RAW', 'VARIANT'),
        ('DESCRIPTION', 'STRING(256)'),
        ('MONITORED_TIME', 'TIMESTAMP_TZ'),
        ('GROUP_ID', 'STRING(30)'),
        ('GROUP_NAME', 'STRING(255)'),
        ('ACCOUNT_ID', 'STRING(30)'),
        ('REGION_NAME', 'STRING(16)'),
        ('VPC_ID', 'STRING(30)')
    ],
    # Elastic Load Balancer landing table
    'ELB': [
        ('RAW', 'VARIANT'),
        ('MONITORED_TIME', 'TIMESTAMP_TZ'),
        ('HOSTED_ZONE_NAME', 'STRING(256)'),
        ('HOSTED_ZONE_NAME_ID', 'STRING(30)'),
        ('CREATED_TIME', 'TIMESTAMP_TZ'),
        ('DNS_NAME', 'STRING(512)'),
        ('LOAD_BALANCER_NAME', 'STRING(256)'),
        ('REGION_NAME', 'STRING(16)'),
        ('SCHEME', 'STRING(30)'),
        ('VPC_ID', 'STRING(30)')
    ]
}


def connect(connection_name, options):
    connection_type = options['connection_type']
    columns = LANDING_TABLES_COLUMNS[connection_type]

    msg = create_asset_table(connection_name, connection_type, columns, options)

    return {
        'newStage': 'finalized',
        'newMessage': msg,
    }


def create_asset_table(connection_name, asset_type, columns, options):
    # create the tables, based on the config type (i.e. SG, EC2, ELB)
    table_name = f'aws_asset_inv_{asset_type}_{connection_name}_connection'
    landing_table = f'data.{table_name}'

    comment = yaml_dump(
        module='aws_inventory',
        **options
    )

    db.create_table(name=landing_table, cols=columns, comment=comment)
    metadata_cols = [
        ('snapshot_at', 'TIMESTAMP_LTZ'),
        ('run_id', 'VARCHAR(100)'),
        ('account_id', 'VARCHAR(100)'),
        ('account_alias', 'VARCHAR(100)'),
        (f'{asset_type}_count', 'NUMBER'),
        ('error', 'VARCHAR')
    ]
    create_metadata_table(table=AWS_ACCOUNTS_METADATA, cols=metadata_cols, addition=metadata_cols[3])
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')

    return f"AWS {asset_type} asset ingestion table created!"


def ingest(table_name, options):
    landing_table = f'data.{table_name}'
    aws_access_key = options.get('aws_access_key')
    aws_secret_key = options.get('aws_secret_key')
    connection_type = options.get('connection_type')
    source_role_arn = options.get('source_role_arn')
    destination_role_name = options.get('destination_role_name')
    external_id = options.get('external_id')
    accounts_identifier = options.get('accounts_identifier')

    ingest_of_type = {
        'EC2': ec2_dispatch,
        'SG': sg_dispatch,
        'ELB': elb_dispatch,
    }[connection_type]

    if source_role_arn and destination_role_name and external_id and accounts_identifier:
        # get accounts list, pass list into ingest ec2
        query = f"SELECT account_id, account_alias FROM data.aws_accounts_{accounts_identifier}_connection WHERE created_at = (select max(created_at) FROM data.aws_accounts_{accounts_identifier}_connection)"
        accounts = db.fetch(query)
        count = ingest_of_type(landing_table, accounts=accounts, source_role_arn=source_role_arn, destination_role_name=destination_role_name, external_id=external_id)

    elif aws_access_key and aws_secret_key:
        count = ingest_of_type(landing_table, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)
        log.info(f'Inserted {count} rows.')
        yield count
    else:
        log.error()


def ec2_dispatch(landing_table, aws_access_key='', aws_secret_key='', accounts=None, source_role_arn='', destination_role_name='', external_id=''):
    if accounts:
        for account in accounts:
            id = account['ACCOUNT_ID']
            name = account['ACCOUNT_ALIAS']
            target_role = f'arn:aws:iam::{id}:role/{destination_role_name}'
            log.info(f"Using role {target_role}")
            try:
                if id == '686874466970':
                    import pdb; pdb.set_trace()
                sts_assume_role(source_role_arn, target_role, external_id)
                results = ingest_ec2(landing_table)

                db.insert(
                    AWS_ACCOUNTS_METADATA, values=[(
                        datetime.utcnow(),
                        RUN_ID,
                        id,
                        name,
                        results
                    )],
                    columns=['snapshot_at', 'run_id', 'account_id', 'account_alias', 'ec2_count']
                )

            except Exception as e:
                db.insert(
                    AWS_ACCOUNTS_METADATA, values=[(
                        datetime.utcnow(),
                        RUN_ID,
                        id,
                        name,
                        0,
                        e
                    )],
                    columns=['snapshot_at', 'run_id', 'account_id', 'account_alias', 'ec2_count', 'error']
                )
                log.error(f"Unable to assume role {target_role} with error", e)
    else:
        ingest_ec2(landing_table, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)


def sg_dispatch(landing_table, aws_access_key='', aws_secret_key='', accounts=None, source_role_arn='', destination_role_name='', external_id=''):
    if accounts:
        for account in accounts:
            id = account['ACCOUNT_ID']
            name = account['ACCOUNT_ALIAS']
            target_role = f'arn:aws:iam::{id}:role/{destination_role_name}'
            log.info(f"Using role {target_role}")
            try:
                sts_assume_role(source_role_arn, target_role, external_id)
                results = ingest_sg(landing_table)

                db.insert(
                    AWS_ACCOUNTS_METADATA, values=[(
                        datetime.utcnow(),
                        id,
                        name,
                        results,
                        None
                    )]
                )
            except Exception as e:
                db.insert(
                    AWS_ACCOUNTS_METADATA, values=[(
                        datetime.utcnow(),
                        id,
                        name,
                        0,
                        e
                    )]
                )
                log.error(f"Unable to assume role {target_role} with error", e)
    else:
        ingest_sg(landing_table, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)


def elb_dispatch(landing_table, aws_access_key='', aws_secret_key='', accounts=None, source_role_arn='', destination_role_name='', external_id=''):
    if accounts:
        for account in accounts:
            id = account['ACCOUNT_ID']
            name = account['ACCOUNT_ALIAS']
            target_role = f'arn:aws:iam::{id}:role/{destination_role_name}'
            log.info(f"Using role {target_role}")
            try:
                sts_assume_role(source_role_arn, target_role, external_id)
                results = ingest_elb(landing_table)

                db.insert(
                    AWS_ACCOUNTS_METADATA, values=[(
                        datetime.utcnow(),
                        id,
                        name,
                        results,
                        None
                    )]
                )
            except Exception as e:
                db.insert(
                    AWS_ACCOUNTS_METADATA, values=[(
                        datetime.utcnow(),
                        id,
                        name,
                        0,
                        e
                    )]
                )
                log.error(f"Unable to assume role {target_role} with error", e)
    else:
        ingest_elb(landing_table, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)


def ingest_ec2(landing_table, aws_access_key=None, aws_secret_key=None):
    instances = get_ec2_instances(aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)

    monitor_time = datetime.utcnow().isoformat()
    db.insert(
        landing_table,
        values=[(
            row,
            row['InstanceId'],
            row['Architecture'],
            monitor_time,
            row['InstanceType'],
            row.get('KeyName', ''),  # can be not present if a managed instance such as EMR
            row['LaunchTime'],
            row['Region']['RegionName'],
            row['State']['Name'],
            row['InstanceName'])
            for row in instances
        ],
        select='PARSE_JSON(column1), column2, column3, column4, column5, column6, column7, column8, column9, column10'
    )

    return len(instances)


def ingest_sg(landing_table, aws_access_key=None, aws_secret_key=None):
    groups = get_all_security_groups(aws_access_key, aws_secret_key)
    monitor_time = datetime.utcnow().isoformat()
    db.insert(
        landing_table,
        values=[(
            row,
            row['Description'],
            monitor_time,
            row['GroupId'],
            row['GroupName'],
            row['OwnerId'],
            row['Region']['RegionName'],
            row['VpcId'])
            for row in groups],
        select='PARSE_JSON(column1), column2, column3, column4, column5, column6, column7, column8'
    )
    return len(groups)


def ingest_elb(landing_table, aws_access_key=None, aws_secret_key=None):
    elbs = get_all_elbs(aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)
    monitor_time = datetime.utcnow().isoformat()

    db.insert(
        landing_table,
        values=[(
            row,
            monitor_time,
            row['CanonicalHostedZoneName'],
            row['CanonicalHostedZoneNameID'],
            row['CreatedTime'],
            row['DNSName'],
            row['LoadBalancerName'],
            row['Region']['RegionName'],
            row['Scheme'],
            row['VPCId'])
            for row in elbs],
        select='PARSE_JSON(column1), column2, column3, column4, column5, column6, '
               'column7, column8, column9, column10'
    )
    return len(elbs)


def get_ec2_instances(aws_access_key=None, aws_secret_key=None):
    client = boto3.client(
        'ec2',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    regions = client.describe_regions()['Regions']

    log.info(f"Searching for EC2 instances in {len(regions)} region(s).")

    # get list of all instances in each region
    instances = []
    for region in regions:
        client = boto3.client('ec2', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region)
        paginator = client.get_paginator('describe_instances')
        page_iterator = paginator.paginate()
        region = [
            instance for page in page_iterator
            for instance_array in page['Reservations']
            for instance in instance_array['Instances']
        ]
        instances.extend(region)

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


def get_all_security_groups(aws_access_key=None, aws_secret_key=None, session=None):
    """
    This function grabs each security group from each region and returns
    a list of the security groups.

    Each security group is manually given a 'Region' field for clarity
    """

    regions = boto3.client(
        'ec2',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    ).describe_regions()['Regions']

    log.info(f"Searching for Security Groups in {len(regions)} region(s).")

    # get list of all groups in each region
    security_groups = []
    for region in regions:
        ec2 = boto3.client(
            'ec2',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region['RegionName']
        )
        for group in ec2.describe_security_groups()['SecurityGroups']:
            group["Region"] = region
            group_str = json.dumps(group, default=datetime_serializer).encode("utf-8")  # for the boto3 datetime fix
            group = json.loads(group_str)
            security_groups.append(group)

    # return list of groups
    log.info(f"Successfully serialized {len(security_groups)} security group(s).")
    return security_groups


def get_all_elbs(aws_access_key=None, aws_secret_key=None):
    v1_elbs = get_all_v1_elbs(aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)
    v2_elbs = get_all_v2_elbs(aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)
    elbs = v1_elbs + v2_elbs

    if len(elbs) is 0:
        log.info("no elastic load balancers found")
        return

    return elbs


def get_all_v1_elbs(aws_access_key=None, aws_secret_key=None):
    """
    This function grabs each classic elb from each region and returns
    a list of them.
    """
    regions = boto3.client(
        'ec2',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    ).describe_regions()['Regions']

    log.info(f"Searching {len(regions)} region(s) for classic load balancers.")

    # get list of all load balancers in each region
    elbs = []
    for region in regions:
        elb_client = boto3.client(
            'elb',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region['RegionName'])
        for elb in elb_client.describe_load_balancers()['LoadBalancerDescriptions']:
            # add region before adding elb to list of elbs
            elb["Region"] = region
            elb_str = json.dumps(elb, default=datetime_serializer).encode("utf-8")  # for the datetime ser fix
            elb = json.loads(elb_str)
            elbs.append(elb)

    # return list of load balancers
    log.info(f"Successfully serialized {len(elbs)} classic elastic load balancers(s).")
    return elbs


def get_all_v2_elbs(aws_access_key=None, aws_secret_key=None):
    """
    This function grabs each v2 elb from each region and returns
    a list of them.
    """
    regions = boto3.client(
        'ec2',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    ).describe_regions()['Regions']

    log.info(f"Searching {len(regions)} region(s) for modern load balancers.")

    # get list of all load balancers in each region
    elbs = []
    for region in regions:
        elb_client = boto3.client(
            'elbv2',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region['RegionName']
        )
        for elb in elb_client.describe_load_balancers()['LoadBalancers']:
            # add region
            elb["Region"] = region

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
