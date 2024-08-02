import asyncio
from inspect import signature
import random
import multiprocessing as mp
import time
from typing import Any

import aioboto3
import boto3
from botocore.parsers import ResponseParserError
from botocore.exceptions import CredentialRetrievalError
import yaml
from requests import auth

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE


class AioRateLimit:
    def __init__(self, pace_per_second=50):
        self.pace_per_second = pace_per_second
        self.interval = 1 / pace_per_second
        self.next_allowed_time = time.monotonic()

    async def wait(self, cost=1):
        now = time.monotonic()
        self.next_allowed_time += self.interval * cost

        if self.next_allowed_time > now:
            await asyncio.sleep(self.next_allowed_time - now)
        else:
            self.next_allowed_time = now

    async def iterate_with_wait(self, async_iterable, cost=1):
        iterator = async_iterable.__aiter__()
        while True:
            await self.wait(cost)
            try:
                item = await iterator.__anext__()
            except StopAsyncIteration:
                break
            yield item

    async def retry(
        self,
        coroutine_factory,
        times=60,
        cost=1,
        seconds_between_retries=1,
        exp_base=0,
        retry_exceptions=(ResponseParserError, CredentialRetrievalError),
    ):
        for i in range(times + 1):
            try:
                await self.wait(cost)
                return await coroutine_factory()
            except retry_exceptions as e:
                if i < times:
                    backoff = exp_base**i if exp_base > 0 else 0
                    sleep_time = seconds_between_retries + backoff
                    await asyncio.sleep(sleep_time)
                else:
                    raise

    async def iterate_with_retry(
        self,
        async_iterable_factory,
        times=60,
        seconds_between_retries=1,
        exp_base=0,
        retry_exceptions=(ResponseParserError, CredentialRetrievalError),
    ):
        for i in range(times + 1):
            try:
                xs = []
                async for x in async_iterable_factory():
                    xs.append(x)
                for x in xs:
                    yield x  # only yield after all succeeded
                break
            except retry_exceptions as e:
                if i < times:
                    backoff = exp_base**i if exp_base > 0 else 0
                    sleep_time = seconds_between_retries + backoff
                    await asyncio.sleep(seconds_between_retries + backoff)
                else:
                    raise


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
    payloads: Any = mp.JoinableQueue()
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


async def aio_sts_assume_role(
    metadata_rate_limit, src_role_arn, dest_role_arn, dest_external_id=None
):
    session_name = ''.join(random.choice('0123456789ABCDEF') for i in range(16))
    await metadata_rate_limit.wait(cost=2)
    async with aioboto3.Session().client('sts') as sts:
        src_role = await metadata_rate_limit.retry(
            lambda: sts.assume_role(RoleArn=src_role_arn, RoleSessionName=session_name)
        )
        await metadata_rate_limit.wait(cost=2)
        async with aioboto3.Session(
            aws_access_key_id=src_role['Credentials']['AccessKeyId'],
            aws_secret_access_key=src_role['Credentials']['SecretAccessKey'],
            aws_session_token=src_role['Credentials']['SessionToken'],
        ).client('sts') as sts_client:
            sts_role = await metadata_rate_limit.retry(
                lambda: sts_client.assume_role(
                    RoleArn=dest_role_arn,
                    RoleSessionName=session_name,
                    ExternalId=dest_external_id,
                )
                if dest_external_id
                else sts_client.assume_role(
                    RoleArn=dest_role_arn, RoleSessionName=session_name
                )
            )

            return (
                sts_role['Credentials']['Expiration'],
                aioboto3.Session(
                    aws_access_key_id=sts_role['Credentials']['AccessKeyId'],
                    aws_secret_access_key=sts_role['Credentials']['SecretAccessKey'],
                    aws_session_token=sts_role['Credentials']['SessionToken'],
                ),
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
