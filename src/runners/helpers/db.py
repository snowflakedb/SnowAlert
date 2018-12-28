"""Helper specific to SnowAlert connecting to the database"""

from typing import List

import snowflake.connector
from snowflake.connector.network import MASTER_TOKEN_EXPIRED_GS_CODE

from . import log
from .auth import load_pkb
from .dbconfig import ACCOUNT, USER, WAREHOUSE, PRIVATE_KEY, PRIVATE_KEY_PASSWORD, TIMEOUT
from .dbconnect import snowflake_connect

CACHED_CONNECTION = None


def retry(f, E=Exception, n=3, log_errors=True):
    while 1:
        try:
            return f()
        except E as e:
            n -= 1
            if log_errors:
                log.error(e)
            if n < 0:
                raise


###
# Connecting
###

def preflight_checks(ctx):
    user_props = dict((x['property'], x['value']) for x in fetch(ctx, f'DESC USER {USER};'))
    assert user_props.get('DEFAULT_ROLE') != 'null', f"default role on user {USER} must not be null"


def connect(run_preflight_checks=True, flush_cache=False):
    global CACHED_CONNECTION
    if CACHED_CONNECTION and not flush_cache:
        return CACHED_CONNECTION

    connect_db, authenticator, pk = (snowflake_connect, 'EXTERNALBROWSER', None) if PRIVATE_KEY is None else \
                                    (snowflake.connector.connect, None, load_pkb(PRIVATE_KEY, PRIVATE_KEY_PASSWORD))

    try:
        connection = retry(lambda: connect_db(
            user=USER,
            account=ACCOUNT,
            private_key=pk,
            authenticator=authenticator,
            ocsp_response_cache_filename='/tmp/.cache/snowflake/ocsp_response_cache',
            network_timeout=TIMEOUT
        ))

        if run_preflight_checks:
            preflight_checks(connection)

    except Exception as e:
        log.fatal(e, "Failed to connect.")

    execute(connection, f'USE WAREHOUSE {WAREHOUSE};')

    CACHED_CONNECTION = connection
    return connection


def fetch(ctx, query):
    res = execute(ctx, query)
    cols = [c[0] for c in res.description]
    while True:
        row = res.fetchone()
        if row is None:
            break
        yield dict(zip(cols, row))


def execute(ctx, query):
    try:
        return ctx.cursor().execute(query)
    except snowflake.connector.errors.ProgrammingError as e:
        if e.errno == int(MASTER_TOKEN_EXPIRED_GS_CODE):
            connect(run_preflight_checks=False, flush_cache=True)
            return execute(query)
        log.error(e, f"Programming Error in query: {query}")
        return ctx.cursor().execute("SELECT 1 WHERE FALSE;")


def connect_and_execute(queries=None):
    connection = connect()

    if type(queries) is str:
        execute(connection, queries)

    if type(queries) is list:
        for q in queries:
            execute(connection, q)

    return connection


def connect_and_fetchall(query):
    ctx = connect()
    return ctx, execute(ctx, query).fetchall()


###
# SnowAlert specific helpers, similar to ORM
###

def load_rules(ctx, postfix) -> List[str]:
    from runners.config import RULES_SCHEMA
    try:
        views = ctx.cursor().execute(f'SHOW VIEWS IN {RULES_SCHEMA}').fetchall()
    except Exception as e:
        log.fatal(e, f"Loading '{postfix}' rules failed.")

    rules = [name[1] for name in views if name[1].lower().endswith(postfix)]
    log.info(f"Loaded {len(views)} views, {len(rules)} were '{postfix}' rules.")

    return rules
