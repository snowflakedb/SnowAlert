import boto3


DISALLOWED_CLIENTS = {'kms', 'secretsmanager'}

def process_row(client_name, method_name, region='us-west-2', **kwargs):
    if client_name in DISALLOWED_CLIENTS:
        return
    method = getattr(boto3.client(client_name, region), method_name)
    return method(**kwargs)
