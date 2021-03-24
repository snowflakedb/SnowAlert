from inspect import signature
import random
import multiprocessing as mp

import aioboto3
import boto3
import yaml
from requests import auth

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE


class Bearer(auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


def updated(d=None, *ds, **kwargs):
    """Shallow merges dictionaries together, mutating + returning first arg"""
    if d is None:
        d = {}
    for new_d in ds:
        if new_d is not None:
            d.update(new_d)
    if kwargs:
        d.update(kwargs)
    return d


def qmap_mp(num_threads, f, args):
    payloads = mp.JoinableQueue()
    procs = []

    def add_task(arg):
        payloads.put(arg)

    def process_task():
        while True:
            payload = payloads.get()
            try:
                f(payload, add_task)
            finally:
                payloads.task_done()

    for arg in args:
        add_task(arg)

    procs = [mp.Process(target=process_task) for _ in range(num_threads)]
    for p in procs:
        p.start()

    payloads.join()
    for p in procs:
        p.kill()


def sts_assume_role(src_role_arn, dest_role_arn, dest_external_id=None):
    session_name = ''.join(random.choice('0123456789ABCDEF') for i in range(16))
    src_role = boto3.client('sts').assume_role(
        RoleArn=src_role_arn, RoleSessionName=session_name
    )
    sts_client = boto3.Session(
        aws_access_key_id=src_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=src_role['Credentials']['SecretAccessKey'],
        aws_session_token=src_role['Credentials']['SessionToken'],
    ).client('sts')

    sts_role = (
        sts_client.assume_role(
            RoleArn=dest_role_arn,
            RoleSessionName=session_name,
            ExternalId=dest_external_id,
        )
        if dest_external_id
        else sts_client.assume_role(RoleArn=dest_role_arn, RoleSessionName=session_name)
    )

    return boto3.Session(
        aws_access_key_id=sts_role['Credentials']['AccessKeyId'],
        aws_secret_access_key=sts_role['Credentials']['SecretAccessKey'],
        aws_session_token=sts_role['Credentials']['SessionToken'],
    )


async def aio_sts_assume_role(src_role_arn, dest_role_arn, dest_external_id=None):
    session_name = ''.join(random.choice('0123456789ABCDEF') for i in range(16))
    async with aioboto3.client('sts') as sts:
        src_role = await sts.assume_role(
            RoleArn=src_role_arn, RoleSessionName=session_name
        )
        async with aioboto3.Session(
            aws_access_key_id=src_role['Credentials']['AccessKeyId'],
            aws_secret_access_key=src_role['Credentials']['SecretAccessKey'],
            aws_session_token=src_role['Credentials']['SessionToken'],
        ).client('sts') as sts_client:
            sts_role = await (
                sts_client.assume_role(
                    RoleArn=dest_role_arn,
                    RoleSessionName=session_name,
                    ExternalId=dest_external_id,
                )
                if dest_external_id
                else sts_client.assume_role(
                    RoleArn=dest_role_arn, RoleSessionName=session_name
                )
            )

            return aioboto3.Session(
                aws_access_key_id=sts_role['Credentials']['AccessKeyId'],
                aws_secret_access_key=sts_role['Credentials']['SecretAccessKey'],
                aws_session_token=sts_role['Credentials']['SessionToken'],
            )


def yaml_dump(**kwargs):
    return yaml.dump(kwargs, default_flow_style=False, explicit_start=True)


def bytes_to_str(x):
    return x.decode() if type(x) is bytes else x


def create_metadata_table(table, cols, addition):
    db.create_table(table, cols, ifnotexists=True)
    db.execute(f"GRANT INSERT, SELECT ON {table} TO ROLE {SA_ROLE}")
    table_names = (row['name'] for row in db.fetch(f'desc table {table}'))
    if any(name == addition[0].upper() for name in table_names):
        return
    db.execute(f'ALTER TABLE {table} ADD COLUMN {addition[0]} {addition[1]}')


def apply_part(f, *args, **kwargs):
    "apply to f args and whatever part of kwargs it has params for"
    params = signature(f).parameters
    return f(*args, **{p: v for p, v in kwargs.items() if p in params})
