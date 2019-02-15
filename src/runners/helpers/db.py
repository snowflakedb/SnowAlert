"""Helper specific to SnowAlert connecting to the database"""

from typing import List
from os import environ

import snowflake.connector
from snowflake.connector.network import MASTER_TOKEN_EXPIRED_GS_CODE, OAUTH_AUTHENTICATOR

from . import log
from .auth import load_pkb, oauth_refresh
from .dbconfig import ACCOUNT, USER, WAREHOUSE, PRIVATE_KEY, PRIVATE_KEY_PASSWORD, TIMEOUT
from .dbconnect import snowflake_connect

CACHED_CONNECTION = None


def retry(f, E=Exception, n=3, log_errors=True, handlers=[]):
    while 1:
        try:
            return f()
        except E as e:
            n -= 1
            for exception, handler in handlers:
                if isinstance(e, exception):
                    return handler(e)
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


def connect(run_preflight_checks=True, flush_cache=False, oauth={}):
    account = oauth.get('account')
    oauth_refresh_token = oauth.get('refresh_token')
    oauth_access_token = oauth_refresh(account, oauth_refresh_token) if oauth_refresh_token else None
    oauth_username = oauth.get('username')
    oauth_account = oauth.get('account')

    global CACHED_CONNECTION
    if CACHED_CONNECTION and not flush_cache and not oauth_access_token:
        return CACHED_CONNECTION

    connect_db, authenticator, pk = \
        (snowflake.connector.connect, OAUTH_AUTHENTICATOR, None) if oauth_access_token else \
        (snowflake_connect, 'EXTERNALBROWSER', None) if PRIVATE_KEY is None else \
        (snowflake.connector.connect, None, load_pkb(PRIVATE_KEY, PRIVATE_KEY_PASSWORD))

    def connect():
        return connect_db(
            user=oauth_username or USER,
            account=oauth_account or ACCOUNT,
            token=oauth_access_token,
            private_key=pk,
            authenticator=authenticator,
            ocsp_response_cache_filename='/tmp/.cache/snowflake/ocsp_response_cache',
            network_timeout=TIMEOUT
        )

    try:
        connection = retry(connect)

        if run_preflight_checks:
            preflight_checks(connection)

        if not oauth_access_token:
            execute(connection, f'USE WAREHOUSE {WAREHOUSE};')

        if not CACHED_CONNECTION and not oauth_access_token:
            CACHED_CONNECTION = connection
        return connection

    except Exception as e:
        log.error(e, "Failed to connect.")


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
            return execute(ctx, query)
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
        log.error(e, f"Loading '{postfix}' rules failed.")
        return []

    rules = [name[1] for name in views if name[1].lower().endswith(postfix)]
    log.info(f"Loaded {len(views)} views, {len(rules)} were '{postfix}' rules.")

    return rules


def insert_alerts(alerts, ctx=None):
    from runners.config import ALERTS_TABLE
    if ctx is None:
        ctx = CACHED_CONNECTION or connect()

    ctx.cursor().execute(
        (
            f'INSERT INTO {ALERTS_TABLE}(alert_time, event_time, alert) '
            f'SELECT PARSE_JSON(column1):ALERT_TIME, PARSE_JSON(column1):EVENT_TIME, PARSE_JSON(column1) '
            f'FROM values {", ".join(["(%s)"] * len(alerts))};'
        ),
        alerts
    )


def insert_violations_query_run(query_name, ctx=None):
    from runners.config import VIOLATIONS_TABLE, RULES_SCHEMA
    if ctx is None:
        ctx = CACHED_CONNECTION or connect()

    output_column = environ.get('output_column', 'result')
    time_column = environ.get('time_column', 'alert_time')
    time_filter_unit = environ.get('time_filter_unit', 'day')
    time_filter_amount = -1 * int(environ.get('time_filter_amount', 1))

    CUTOFF_TIME = f'DATEADD({time_filter_unit}, {time_filter_amount}, CURRENT_TIMESTAMP())'

    log.info(f"{query_name} processing...")
    try:
        result = ctx.cursor().execute(
            f"""
            INSERT INTO {VIOLATIONS_TABLE} ({time_column}, {output_column})
                SELECT alert_time, OBJECT_CONSTRUCT(*)
                FROM {RULES_SCHEMA}.{query_name}
                WHERE alert_time > {CUTOFF_TIME}
            ;
            """
        ).fetchall()
        log.info(f"{query_name} created {result[0][0]} rows.")

    except Exception as e:
        log.info(f"{query_name} run threw an exception:", e)

    return ctx
