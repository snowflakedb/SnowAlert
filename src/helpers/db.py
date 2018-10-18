"""Helper specific to SnowAlert connecting to the database"""

import argparse
from base64 import b64decode
import os
from typing import List

import snowflake.connector

from helpers import log

from .auth import load_pkb

parser = argparse.ArgumentParser(description="Optionally takes in a password")
parser.add_argument("-p", "--private_key_password", type=str, help="Pass in a password on the command line")
args = parser.parse_args()


def retry(f, E=Exception, n=3):
    while 1:
        try:
            return f()
        except E:
            n -= 1
            if n < 0:
                raise


###
# Connecting
###

def connect():
    p8_private_key = b64decode(os.environ['PRIVATE_KEY'])
    encrypted_pass = (args.private_key_password or os.environ['PRIVATE_KEY_PASSWORD']).encode('utf-8')

    user = os.environ['SA_USER']
    region = os.environ.get('REGION', 'us-west-2')
    account = os.environ['SNOWFLAKE_ACCOUNT'] + ('' if region == 'us-west-2' else f'.{region}')

    try:
        connection = retry(lambda: snowflake.connector.connect(
            user=user,
            account=account,
            private_key=load_pkb(p8_private_key, encrypted_pass),
            ocsp_response_cache_filename='/tmp/.cache/snowflake/ocsp_response_cache'
        ))

    except Exception as e:
        log.fatal(e, "Failed to connect.")

    return connection


def connect_and_execute(queries=None):
    connection = connect()

    if type(queries) is str:
        connection.cursor().execute(queries)

    if type(queries) is list:
        for q in queries:
            connection.cursor().execute(q)

    return connection


def connect_and_fetchall(query):
    ctx = connect()
    try:
        return ctx, ctx.cursor().execute(query).fetchall()
    except Exception as e:
        log.fatal(f"Execution failed.\nQuery: {query}", e)


###
# Looking
###

def load_rules(ctx, postfix) -> List[str]:
    from config import RULES_SCHEMA
    try:
        views = ctx.cursor().execute(f'SHOW VIEWS IN {RULES_SCHEMA}').fetchall()
    except Exception as e:
        log.fatal(e, f"Loading '{postfix}' rules failed.")

    rules = [name[1] for name in views if name[1].lower().endswith(postfix)]
    print(f"Loaded {len(rules)} '{postfix}' rules.")

    return rules
