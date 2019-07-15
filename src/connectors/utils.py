from base64 import b64encode
import boto3
from os import urandom


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
