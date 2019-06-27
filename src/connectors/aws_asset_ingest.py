"""AWS Asset Inventory
Collects AWS EC2, SG, ELB details into a columnar table
"""
import datetime
import json
import os
import boto3
from datetime import datetime
from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

CONNECTION_OPTIONS = [
    {
        'type': 'select',
        'options': ['EC2', 'SG', 'ELB'],
        'name': 'Asset_Source',
        'title': 'Asset Type',
        'prompt': 'The type of AWS asset information you are ingesting to Snowflake.',
        'required': True
    },
    {
        # The AWS Client ID. The account ID is not necessary as Client ID's are globally unique
        'name': 'Client_ID',
        'type': 'str',
        'required': True
    },
    {
        # The AWS Secret Key
        'name': 'Secret_Key',
        'type': 'str',
        'secret': True,
        'required': True
    }
]

# Define the columns for the EC2 Instances Landing Table
AWS_EC2_LANDING_TABLE_COLUMNS = [
    ('RAW_DATA', 'VARIANT'),
    ('INSTANCE_ID', 'STRING(30)'),
    ('ARCHITECTURE', 'STRING(16)'),
    ('MONITORED_TIME_UTC', 'TIMESTAMP_TZ'),
    ('INSTANCE_TYPE', 'STRING(256)'),
    ('KEY_NAME', 'STRING(256)'),
    ('LAUNCH_TIME', 'TIMESTAMP_TZ'),
    ('REGION_NAME', 'STRING(16)'),
    ('INSTANCE_STATE', 'STRING(16)'),
    ('INSTANCE_NAME', 'STRING(256)')
]

# Define the columns for the Security Group Landing table
AWS_SG_LANDING_TABLE_COLUMNS = [
    ('RAW_DATA', 'VARIANT'),
    ('DESCRIPTION', 'STRING(256)'),
    ('MONITORED_TIME', 'TIMESTAMP_TZ'),
    ('GROUP_ID', 'STRING(30)'),
    ('GROUP_NAME', 'STRING(255)'),
    ('ACCOUNT_ID', 'STRING(30)'),
    ('REGION_NAME', 'STRING(16)'),
    ('VPC_ID', 'STRING(30)')
]

# Define the columns for the Elastic Load Balancer landing table
AWS_ELB_LANDING_TABLE_COLUMNS = [
    ('RAW_DATA', 'VARIANT'),
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


def connect(connection_name, options):

    result = ''

    if options["Asset_Source"] == 'EC2':
        result = create_config_table(connection_name, 'EC2', AWS_EC2_LANDING_TABLE_COLUMNS, options)
    elif options["Asset_Source"] == 'SG':
        result = create_config_table(connection_name, 'SG', AWS_SG_LANDING_TABLE_COLUMNS, options)
    elif options["Asset_Source"] == 'ELB':
        result = create_config_table(connection_name, 'ELB', AWS_ELB_LANDING_TABLE_COLUMNS, options)

    return {
        'newStage': 'finalized',
        'newMessage': f"{result}",
    }


def create_config_table(connection_name, config_type, columns, options):
    # create the tables, based on the config type (i.e. SG, EC2, ELB)
    table_name = f'aws_asset_inv_{config_type}_{connection_name}_connection'
    landing_table = f'data.{table_name}'
    client_id = options["Client_ID"]
    secret_key = options["Secret_Key"]

    comment = f"""
---
module: aws_asset_ingest
client_id: {client_id}
secret_key: {secret_key}
"""

    db.create_table(name=landing_table, cols=columns, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')

    return f'AWS {config_type} asset ingestion table created!'


def ingest(table_name, options):
    landing_table = f'data.{table_name}'
    client_id = options["client_id"]
    secret_key = options["secret_key"]

    regions = boto3.client(
            'ec2',
            aws_access_key_id=client_id,
            aws_secret_access_key=secret_key
    ).describe_regions()['Regions']

    count = 0
    if 'EC2' in table_name:
        count = ingest_ec2(client_id, secret_key, landing_table, regions)
    if 'SG' in table_name:
        count = ingest_sg(client_id, secret_key, landing_table, regions)
    if 'ELB' in table_name:
        count = ingest_elb(client_id, secret_key, landing_table, regions)

    log.info(f'Inserted {count} rows.')
    yield count


def ingest_ec2(client_id, secret_key, landing_table, regions):
    instances = get_ec2_instances(client_id, secret_key, regions)
    monitor_time = datetime.utcnow().isoformat()
    db.insert(
        landing_table,
        values=[(
            row,
            row['InstanceId'],
            row['Architecture'],
            monitor_time,
            row['InstanceType'],
            row.get('KeyName', 'n/a'),  # can be not present if a managed instance such as EMR
            row['LaunchTime'],
            row['Region']['RegionName'],
            row['State']['Name'],
            row['InstanceName'])
            for row in instances
        ],
        select='PARSE_JSON(column1), column2, column3, column4, column5, column6, column7, column8, column9, column10'
    )
    return len(instances)


def ingest_sg(client_id, secret_key, landing_table, regions):
    groups = get_all_security_groups(client_id, secret_key, regions)
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


def ingest_elb(client_id, secret_key, landing_table, regions):
    elbs = get_all_elbs(client_id, secret_key, regions)
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


def get_ec2_instances(client_id, secret_key, regions):
    log.info(f"Searching for EC2 instances in {len(regions)} region(s).")

    # get list of all instances in each region
    instances = []
    for region in regions:
        reservations = boto3.client(
            'ec2',
            aws_access_key_id=client_id,
            aws_secret_access_key=secret_key,
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
    try:
        for tag in instance["Tags"]:
            if "Name" == tag["Key"]:
                return tag["Value"]
    except Exception as e:
        log.info(f"Could not extract EC2 instance name from [{instance}]")

    return ""


def get_all_security_groups(client_id, secret_key, regions):
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
            aws_access_key_id=client_id,
            aws_secret_access_key=secret_key,
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


def get_all_elbs(client_id, secret_key, regions):
    v1_elbs = get_all_v1_elbs(client_id, secret_key, regions)
    v2_elbs = get_all_v2_elbs(client_id, secret_key, regions)
    elbs = v1_elbs + v2_elbs

    if len(elbs) is 0:
        log.info("no elastic load balancers found")
        return

    return elbs


def get_all_v1_elbs(client_id, secret_key, regions):
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
            aws_access_key_id=client_id,
            aws_secret_access_key=secret_key,
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


def get_all_v2_elbs(client_id, secret_key, regions):
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
            aws_access_key_id=client_id,
            aws_secret_access_key=secret_key,
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


def test(name):
    yield {
        'check': 'everything works',
        'success': True,
    }
