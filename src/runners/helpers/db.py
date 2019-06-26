"""Helper specific to SnowAlert connecting to the database"""
from datetime import datetime
import json
from threading import local
import time
from typing import List, Tuple
from os import getpid
from re import match

import snowflake.connector
from snowflake.connector.constants import FIELD_TYPES
from snowflake.connector.network import MASTER_TOKEN_EXPIRED_GS_CODE, OAUTH_AUTHENTICATOR

from . import log
from .auth import load_pkb, oauth_refresh
from .dbconfig import ACCOUNT, ROLE, DATABASE, USER, WAREHOUSE, PRIVATE_KEY, PRIVATE_KEY_PASSWORD, TIMEOUT
from .dbconnect import snowflake_connect

from runners import utils

CACHE = local()
CONNECTION = f'connection-{getpid()}'


def retry(f, E=Exception, n=3, log_errors=True, handlers=[], sleep_seconds_btw_retry=0):
    while 1:
        try:
            return f()
        except E as e:
            n -= 1
            if sleep_seconds_btw_retry > 0:
                time.sleep(sleep_seconds_btw_retry)
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

def connect(flush_cache=False, set_cache=False, oauth={}):
    account = oauth.get('account')
    oauth_refresh_token = oauth.get('refresh_token')
    oauth_access_token = oauth_refresh(account, oauth_refresh_token) if oauth_refresh_token else None
    oauth_username = oauth.get('username')
    oauth_account = oauth.get('account')

    cached_connection = getattr(CACHE, CONNECTION, None)
    if cached_connection and not flush_cache and not oauth_access_token:
        return cached_connection

    connect_db, authenticator, pk = \
        (snowflake.connector.connect, OAUTH_AUTHENTICATOR, None) if oauth_access_token else \
        (snowflake_connect, 'EXTERNALBROWSER', None) if PRIVATE_KEY is None else \
        (snowflake.connector.connect, None, load_pkb(PRIVATE_KEY, PRIVATE_KEY_PASSWORD))

    def connect():
        return connect_db(
            account=oauth_account or ACCOUNT,
            database=DATABASE,
            user=oauth_username or USER,
            warehouse=None if oauth_access_token else WAREHOUSE,
            role=None if oauth_access_token else ROLE,
            token=oauth_access_token,
            private_key=pk,
            authenticator=authenticator,
            ocsp_response_cache_filename='/tmp/.cache/snowflake/ocsp_response_cache',
            network_timeout=TIMEOUT
        )

    try:
        connection = retry(connect)

        # see SP-1116 for why set_cache=False by default
        if set_cache and not cached_connection:
            setattr(CACHE, CONNECTION, connection)

        return connection

    except Exception as e:
        log.error(e, "Failed to connect.")


def fetch(ctx, query=None, fix_errors=True, params=None):
    if query is None:  # TODO(andrey): swap args and refactor
        ctx, query = connect(), ctx

    res = execute(ctx, query, fix_errors, params)
    cols = [c[0] for c in res.description]
    types = [FIELD_TYPES[c[1]] for c in res.description]
    while True:
        row = res.fetchone()
        if row is None:
            break

        def parse_field(value, field_type):
            if value is not None and field_type['name'] in {'OBJECT', 'ARRAY', 'VARIANT'}:
                return json.loads(value)
            return value

        yield {c: parse_field(r, t) for (c, r, t) in zip(cols, row, types)}


def execute(ctx, query=None, fix_errors=True, params=None):
    # TODO(andrey): don't ignore errors by default
    if query is None:  # TODO(andrey): swap args and refactor
        ctx, query = connect(), ctx

    if ctx is None:
        ctx = connect()

    try:
        return ctx.cursor().execute(query, params=params)

    except snowflake.connector.errors.ProgrammingError as e:
        if e.errno == int(MASTER_TOKEN_EXPIRED_GS_CODE):
            connect(flush_cache=True)
            return execute(ctx, query, fix_errors, params)

        if not fix_errors:
            log.debug(f"re-raising error '{e}' in query >{query}<")
            raise

        log.info(e, f"ignoring error '{e}' in query >{query}<")

        return ctx.cursor().execute("SELECT 1 WHERE FALSE;")


def connect_and_execute(queries=None):
    connection = connect()

    if type(queries) is str:
        execute(queries)

    if type(queries) is list:
        for q in queries:
            execute(q)

    return connection


def connect_and_fetchall(query):
    ctx = connect()
    return ctx, execute(query).fetchall()


def fetch_latest(table, col):
    ts = next(fetch(f'SELECT {col} FROM {table} ORDER BY {col} DESC LIMIT 1'), None)
    return ts[col.upper()] if ts else None


def fetch_props(sql, filter=None):
    return {
        row['property']: row['property_value'] for row in fetch(sql) if (
            filter is None
            or row['property'] in filter
        )
    }


###
# SnowAlert specific helpers, similar to ORM
###

def is_valid_rule_name(rule_name):
    valid_ending = (
        rule_name.endswith("_ALERT_QUERY")
        or rule_name.endswith("_ALERT_SUPPRESSION")
        or rule_name.endswith("_VIOLATION_QUERY")
        or rule_name.endswith("_VIOLATION_SUPPRESSION")
        or rule_name.endswith("_POLICY_DEFINITION")
    )

    # \w is equivalent to [a-zA-Z0-9_]
    no_injection = match(r'^\w+$', rule_name)

    return no_injection and valid_ending


def load_rules(postfix) -> List[str]:
    try:
        views = sorted(
            (v['name'] for v in fetch(f'SHOW VIEWS IN rules')),
            key=lambda vn: vn.replace('_', '{{')  # _ after letters, like in Snowflake
        )
    except Exception as e:
        log.error(e, f"Loading '{postfix}' rules failed.")
        return []

    rules = [vn for vn in views if is_valid_rule_name(vn) and vn.endswith(postfix)]
    log.info(f"Loaded {len(views)} views, {len(rules)} were '{postfix}' rules.")

    return rules


INSERT_ALERTS_QUERY = f"""
INSERT INTO results.alerts (alert_time, event_time, alert)
SELECT PARSE_JSON(column1):ALERT_TIME
     , PARSE_JSON(column1):EVENT_TIME
     , PARSE_JSON(column1)
FROM VALUES {{values}}
"""


def sql_value_placeholders(n):
    return ", ".join(["(%s)"] * n)


def insert(table, values, overwrite=False, select=""):
    if len(values) == 0:
        return

    if select:
        select = f'SELECT {select} FROM '

    overwrite = ' OVERWRITE' if overwrite else ''

    sql = (
        f"INSERT{overwrite}\n"
        f"  INTO {table}\n"
        f"  {select}VALUES {sql_value_placeholders(len(values))}\n"
        f";"
    )

    jsony = (dict, list, tuple)
    params_with_json = [
        [utils.json_dumps(v) if isinstance(v, jsony) else v for v in vp]
        for vp in values
    ]

    return execute(sql, params=params_with_json, fix_errors=False)


def insert_alerts(alerts, ctx=None):
    if ctx is None:
        ctx = connect()

    query = INSERT_ALERTS_QUERY.format(values=sql_value_placeholders(len(alerts)))
    return ctx.cursor().execute(query, alerts)


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
            'ENVIRONMENT', IFNULL(OBJECT_CONSTRUCT(*):ENVIRONMENT, PARSE_JSON('null')),
            'OBJECT', IFNULL(OBJECT_CONSTRUCT(*):OBJECT, PARSE_JSON('null')),
            'OWNER', IFNULL(OBJECT_CONSTRUCT(*):OWNER, PARSE_JSON('null')),
            'TITLE', IFNULL(OBJECT_CONSTRUCT(*):TITLE, PARSE_JSON('null')),
            'ALERT_TIME', IFNULL(OBJECT_CONSTRUCT(*):ALERT_TIME, PARSE_JSON('null')),
            'DESCRIPTION', IFNULL(OBJECT_CONSTRUCT(*):DESCRIPTION, PARSE_JSON('null')),
            'EVENT_DATA', IFNULL(OBJECT_CONSTRUCT(*):EVENT_DATA, PARSE_JSON('null')),
            'DETECTOR', IFNULL(OBJECT_CONSTRUCT(*):DETECTOR, PARSE_JSON('null')),
            'SEVERITY', IFNULL(OBJECT_CONSTRUCT(*):SEVERITY, PARSE_JSON('null')),
            'QUERY_ID', IFNULL(OBJECT_CONSTRUCT(*):QUERY_ID, PARSE_JSON('null')),
            'QUERY_NAME', '{{query_name}}'
        )
      )
    ))
  , OBJECT_CONSTRUCT(*)
FROM rules.{{query_name}}
WHERE IFF(alert_time IS NOT NULL, alert_time > {{CUTOFF_TIME}}, TRUE)
"""


def insert_violations_query_run(query_name, ctx=None) -> Tuple[int, int]:
    if ctx is None:
        ctx = connect()

    CUTOFF_TIME = f'DATEADD(day, -1, CURRENT_TIMESTAMP())'

    log.info(f"{query_name} processing...")
    try:
        result = next(fetch(INSERT_VIOLATIONS_WITH_ID_QUERY.format(**locals()), fix_errors=False))
    except Exception:
        log.info('warning: missing STRING ID column in RESULTS.VIOLATIONS')
        result = next(fetch(INSERT_VIOLATIONS_QUERY.format(**locals()), fix_errors=False))

    num_rows_inserted = result['number of rows inserted']
    log.info(f"{query_name} created {num_rows_inserted} rows.")
    return num_rows_inserted


def value_to_sql(v):
    if type(v) is str:
        return f"'{v}'"
    return str(v)


def get_alerts(**kwargs):
    predicates = '\n  AND '.join(f'{k}={value_to_sql(v)}' for k, v in kwargs.items())
    where_clause = f'WHERE {predicates}' if predicates else ''
    return fetch(f"SELECT * FROM data.alerts {where_clause}")


def record_metadata(metadata, table, e=None):
    ctx = connect()

    if e is None and 'EXCEPTION' in metadata:
        e = metadata['EXCEPTION']
        del metadata['EXCEPTION']

    if e is not None:
        exception_only = utils.format_exception_only(e)
        metadata['ERROR'] = {
            'EXCEPTION': utils.format_exception(e),
            'EXCEPTION_ONLY': exception_only,
        }
        if exception_only.startswith('snowflake.connector.errors.ProgrammingError: '):
            metadata['ERROR']['PROGRAMMING_ERROR'] = exception_only[45:]

    metadata.setdefault('ROW_COUNT', {'INSERTED': 0, 'UPDATED': 0, 'SUPPRESSED': 0, 'PASSED': 0})

    metadata['END_TIME'] = datetime.utcnow()
    metadata['DURATION'] = str(metadata['END_TIME'] - metadata['START_TIME'])
    metadata['START_TIME'] = str(metadata['START_TIME'])
    metadata['END_TIME'] = str(metadata['END_TIME'])

    record_type = metadata.get('QUERY_NAME', 'RUN')

    metadata_json_sql = "'" + json.dumps(metadata).replace('\\', '\\\\').replace("'", "\\'") + "'"

    sql = f'''
    INSERT INTO {table}(event_time, v)
    SELECT '{metadata['START_TIME']}'
         , PARSE_JSON(column1)
    FROM VALUES({metadata_json_sql})
    '''

    try:
        ctx.cursor().execute(sql)
        log.info(f"{record_type} metadata recorded.")

    except Exception as e:
        log.error(f"{record_type} metadata failed to log.", e)


def record_failed_ingestion(table, r, timestamp):
    log = json.dumps({'headers': dict(r.headers),
                      'text': r.text,
                      'status_code': r.status_code,
                      'failure': True})
    data = [(log, timestamp)]
    query = f"INSERT INTO {table} SELECT PARSE_JSON(COLUMN1), COLUMN2 FROM VALUES (%s)"
    execute(query, params=data)


def get_pipes(schema):
    return fetch(f"SHOW PIPES IN {schema}")


def create_stage(name, url, prefix, cloud, credentials, file_format, replace=False, comment=''):
    replace = 'OR REPLACE ' if replace else ''
    query = f"CREATE {replace}STAGE {name} \nURL='{url}/{prefix}' "
    if cloud == 'aws':
        query += f"\nCREDENTIALS=(aws_role = '{credentials}') "
    elif cloud == 'azure':
        query += f"\nCREDENTIALS=(azure_sas_token = '{credentials}') "
    query += f"\nFILE_FORMAT=({file_format}) \nCOMMENT='{comment}'"
    execute(query, fix_errors=False)


def create_table(name, cols, replace=False, comment=''):
    replace = 'OR REPLACE ' if replace else ''
    comment = f"\nCOMMENT='{comment}' " if comment else ''
    columns = '('
    for pair in cols:
        columns += f'{pair[0]} {pair[1]}, '
    columns = columns[:-2] + ')'
    query = f"CREATE {replace}TABLE {name}{columns}{comment}"
    execute(query, fix_errors=False)


def create_stream(name, target, replace='', comment=''):
    replace = 'OR REPLACE ' if replace else ''
    comment = f"\nCOMMENT='{comment} '" if comment else ''
    query = f"CREATE {replace}STREAM {name} {comment}\nON TABLE {target}"
    execute(query, fix_errors=False)


def create_pipe(name, sql, replace='', autoingest='', comment=''):
    replace = 'OR REPLACE ' if replace else ''
    autoingest = 'AUTO_INGEST=TRUE ' if autoingest else ''
    comment = f"\nCOMMENT='{comment} '" if comment else ''
    query = f"CREATE {replace}PIPE {name} {autoingest}{comment} AS \n{sql}"
    execute(query, fix_errors=False)


def create_task(name, schedule, warehouse, sql, replace='', comment=''):
    replace = 'OR REPLACE ' if replace else ''
    schedule = f"SCHEDULE='{schedule}'\n"
    warehouse = f"WAREHOUSE={warehouse}\n"
    comment = f"\nCOMMENT='{comment} '" if comment else ''
    query = f"CREATE {replace}TASK {name} {schedule} {warehouse} {comment} AS \n{sql}"
    execute(query, fix_errors=False)
    execute(f"ALTER TASK {name} RESUME")
