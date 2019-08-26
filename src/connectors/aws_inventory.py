"""AWS Asset Inventory
Collect AWS EC2, SG, ELB details using an Access Key
"""
from datetime import datetime
import json

import boto3

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

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
        'prompt': "This key id will be used to authenticate to AWS.",
        'type': 'str',
        'required': True
    },
    {
        # The AWS Secret Key
        'name': 'aws_secret_key',
        'title': "AWS Secret Key",
        'prompt': "This secret key will be used to authenticate to AWS.",
        'type': 'str',
        'secret': True,
        'required': True
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
    aws_access_key = options['aws_access_key']
    aws_secret_key = options['aws_secret_key']

    comment = f'''
---
module: aws_inventory
aws_access_key: {aws_access_key}
aws_secret_key: {aws_secret_key}
'''

    db.create_table(name=landing_table, cols=columns, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')

    return f"AWS {asset_type} asset ingestion table created!"


def ingest(table_name, options):
    landing_table = f'data.{table_name}'
    aws_access_key = options['aws_access_key']
    aws_secret_key = options['aws_secret_key']
    connection_type = options['connection_type']

    regions = boto3.client(
        'ec2',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    ).describe_regions()['Regions']

    ingest_of_type = {
        'EC2': ingest_ec2,
        'SG': ingest_sg,
        'ELB': ingest_elb,
    }[connection_type]

    count = ingest_of_type(aws_access_key, aws_secret_key, landing_table, regions)
    log.info(f'Inserted {count} rows.')
    yield count


def ingest_ec2(aws_access_key, aws_secret_key, landing_table, regions):
    instances = get_ec2_instances(aws_access_key, aws_secret_key, regions)
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


def ingest_sg(aws_access_key, aws_secret_key, landing_table, regions):
    groups = get_all_security_groups(aws_access_key, aws_secret_key, regions)
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


def ingest_elb(aws_access_key, aws_secret_key, landing_table, regions):
    elbs = get_all_elbs(aws_access_key, aws_secret_key, regions)
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


def get_ec2_instances(aws_access_key, aws_secret_key, regions):
    log.info(f"Searching for EC2 instances in {len(regions)} region(s).")

    # get list of all instances in each region
    instances = []
    for region in regions:
        reservations = boto3.client(
            'ec2',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region['RegionName']
        ).describe_instances()["Reservations"]

        for reservation in reservations:

            for instance in reservation['Instances']:
                instance["Region"] = region
                instance["InstanceName"] = get_ec2_instance_name(instance)
                # for the boto3 datetime fix
                instance_str = json.dumps(instance, default=datetime_serializer).encode("utf-8")
                instance = json.loads(instance_str)
                instances.append(instance)

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


def get_all_security_groups(aws_access_key, aws_secret_key, regions):
    """
    This function grabs each security group from each region and returns
    a list of the security groups.

    Each security group is manually given a 'Region' field for clarity
    """
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


def get_all_elbs(aws_access_key, aws_secret_key, regions):
    v1_elbs = get_all_v1_elbs(aws_access_key, aws_secret_key, regions)
    v2_elbs = get_all_v2_elbs(aws_access_key, aws_secret_key, regions)
    elbs = v1_elbs + v2_elbs

    if len(elbs) is 0:
        log.info("no elastic load balancers found")
        return

    return elbs


def get_all_v1_elbs(aws_access_key, aws_secret_key, regions):
    """
    This function grabs each classic elb from each region and returns
    a list of them.
    """
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


def get_all_v2_elbs(aws_access_key, aws_secret_key, regions):
    """
    This function grabs each v2 elb from each region and returns
    a list of them.
    """
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
