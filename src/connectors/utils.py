from base64 import b64encode
import boto3
from os import urandom
import yaml

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE


def sts_assume_role(src_role_arn, dest_role_arn, dest_external_id=None):
    session_name = b64encode(urandom(18)).decode('utf-8')
    src_role = boto3.client('sts').assume_role(
        RoleArn=src_role_arn,
        RoleSessionName=session_name
    )
    return boto3.Session(
        aws_access_key_id=src_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=src_role['Credentials']['SecretAccessKey'],
        aws_session_token=src_role['Credentials']['SessionToken']
    ).client('sts').assume_role(
        RoleArn=dest_role_arn,
        RoleSessionName=session_name,
        ExternalId=dest_external_id
    )


def yaml_dump(**kwargs):
    return yaml.dump(kwargs, default_flow_style=True, explicit_start=True)


def create_metadata_table(table, cols, addition):
    db.create_table(table, cols, ifnotexists=True)
    db.execute(f"GRANT INSERT, SELECT ON {table} TO ROLE {SA_ROLE}")
    table_names = (row['name'] for row in db.fetch(f'desc table {table}'))
    if any(name == addition[0] for name in table_names):
        return
    db.execute(f'ALTER TABLE {table} ADD COLUMN {addition[0]} {addition[1]}')
