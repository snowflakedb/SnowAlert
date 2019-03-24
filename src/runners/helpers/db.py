"""Helper specific to SnowAlert connecting to the database"""

from typing import List, Tuple
from os import path

import snowflake.connector
from snowflake.connector.network import MASTER_TOKEN_EXPIRED_GS_CODE, OAUTH_AUTHENTICATOR

from . import log
from .auth import load_pkb, oauth_refresh
from .dbconfig import ACCOUNT, DATABASE, USER, WAREHOUSE, PRIVATE_KEY, PRIVATE_KEY_PASSWORD, TIMEOUT
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

        execute(connection, f'USE DATABASE {DATABASE}')
        if not oauth_access_token:
            execute(connection, f'USE WAREHOUSE {WAREHOUSE}')

        if not CACHED_CONNECTION and not oauth_access_token:
            CACHED_CONNECTION = connection
        return connection

    except Exception as e:
        log.error(e, "Failed to connect.")


def fetch(ctx, query=None):
    if query is None:  # TODO(andrey): swap args and refactor
        ctx, query = CACHED_CONNECTION, ctx

    res = execute(ctx, query)
    cols = [c[0] for c in res.description]
    while True:
        row = res.fetchone()
        if row is None:
            break
        yield dict(zip(cols, row))


def execute(ctx, query=None):
    if query is None:  # TODO(andrey): swap args and refactor
        ctx, query = CACHED_CONNECTION, ctx

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
    try:
        views = ctx.cursor().execute(f'SHOW VIEWS IN rules').fetchall()
    except Exception as e:
        log.error(e, f"Loading '{postfix}' rules failed.")
        return []

    rules = [name[1] for name in views if name[1].endswith(postfix)]
    log.info(f"Loaded {len(views)} views, {len(rules)} were '{postfix}' rules.")

    return rules


def insert_alerts(alerts, ctx=None):
    if ctx is None:
        ctx = CACHED_CONNECTION or connect()

    ctx.cursor().execute(
        (
            f'INSERT INTO restuls.alerts (alert_time, event_time, alert) '
            f'SELECT PARSE_JSON(column1):ALERT_TIME, PARSE_JSON(column1):EVENT_TIME, PARSE_JSON(column1) '
            f'FROM values {", ".join(["(%s)"] * len(alerts))};'
        ),
        alerts
    )


def insert_alerts_query_run(query_name, from_time_sql, to_time_sql='CURRENT_TIMESTAMP()', ctx=None):
    if ctx is None:
        ctx = CACHED_CONNECTION or connect()

    log.info(f"{query_name} processing...")

    try:
        pwd = path.dirname(path.realpath(__file__))
        sql = open(f'{pwd}/insert-alert-query.sql.fmt').read().format(
            query_name=query_name,
            from_time_sql=from_time_sql,
            to_time_sql=to_time_sql,
        )
        result = ctx.cursor().execute(sql).fetchall()
        created_count, updated_count = result[0]
        log.info(f"{query_name} created {created_count}, updated {updated_count} rows.")
        return created_count, updated_count

    except Exception as e:
        log.info(f"{query_name} run threw an exception:", e)
        return 0, 0


INSERT_VIOLATIONS_QUERY = f"""
INSERT INTO results.violations (alert_time, result)
SELECT alert_time, OBJECT_CONSTRUCT(*)
FROM rules.{{query_name}}
WHERE alert_time > {{CUTOFF_TIME}}
"""

INSERT_VIOLATIONS_WITH_ID_QUERY = f"""
INSERT INTO results.violations (alert_time, id, result)
SELECT CURRENT_TIMESTAMP()
  , MD5(TO_JSON(
      IFNULL(
        OBJECT_CONSTRUCT(*):IDENTITY,
        OBJECT_CONSTRUCT(
            'ENVIRONMENT', IFNULL(environment, PARSE_JSON('null')),
            'OBJECT', IFNULL(object, PARSE_JSON('null')),
            'TITLE', IFNULL(title, PARSE_JSON('null')),
            'ALERT_TIME', IFNULL(alert_time, PARSE_JSON('null')),
            'DESCRIPTION', IFNULL(description, PARSE_JSON('null')),
            'EVENT_DATA', IFNULL(event_data, PARSE_JSON('null')),
            'DETECTOR', IFNULL(detector, PARSE_JSON('null')),
            'SEVERITY', IFNULL(severity, PARSE_JSON('null')),
            'QUERY_ID', IFNULL(query_id, PARSE_JSON('null')),
            'QUERY_NAME', IFNULL(query_name, PARSE_JSON('null'))
        )
      )
    ))
  , OBJECT_CONSTRUCT(*)
FROM rules.{{query_name}}
WHERE IFF(alert_time IS NOT NULL, alert_time > {{CUTOFF_TIME}}, TRUE)
"""


def insert_violations_query_run(query_name, ctx=None) -> Tuple[int, int]:
    if ctx is None:
        ctx = CACHED_CONNECTION or connect()

    CUTOFF_TIME = f'DATEADD(day, -1, CURRENT_TIMESTAMP())'

    log.info(f"{query_name} processing...")
    try:
        try:
            result = next(fetch(ctx, INSERT_VIOLATIONS_WITH_ID_QUERY.format(**locals())))
        except Exception:
            log.info('warning: missing STRING ID column in RESULTS.VIOLATIONS')
            result = next(fetch(ctx, INSERT_VIOLATIONS_QUERY.format(**locals())))

        num_rows_inserted = result['number of rows inserted']
        log.info(f"{query_name} created {num_rows_inserted} rows, updated 0 rows.")
        log.info(f"{query_name} done.")
        return num_rows_inserted, 0

    except Exception as e:
        log.info(f"{query_name} run threw an exception:", e)
        return 0, 0
