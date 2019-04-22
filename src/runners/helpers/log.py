import datetime
import json
import sys
import traceback

import boto3

from ..config import ENV


def format_exception(e):
    return ''.join(traceback.format_exception(type(e), e, e.__traceback__))


def format_exception_only(e):
    return ''.join(traceback.format_exception_only(type(e), e)).strip()


def write(*args, stream=sys.stdout):
    for a in args:
        if isinstance(a, Exception):
            template = '{fs.filename}:{fs.lineno} in {fs.name}\n  {fs.line}\n'
            trace = traceback.extract_tb(a.__traceback__)
            fmt_trace = ''.join(template.format(fs=f) for f in trace)
            stack = traceback.extract_stack()
            for i, f in enumerate(reversed(stack)):
                if (f.filename, f.name) == (trace[0].filename, trace[0].name):
                    stack = stack[:-i]
                    break
            fmt_stack = ''.join(template.format(fs=f) for f in stack)

            a = fmt_stack + '--- got trace ---\n' + fmt_trace
        print(a, file=stream, flush=True)


def debug(*args):
    if ENV in ('dev', 'test'):
        write(*args, stream=sys.stdout)


def info(*args):
    write(*args, stream=sys.stdout)


def error(*args):
    write(*args, stream=sys.stderr)


def fatal(*args):
    error(*args)
    sys.exit(1)


def metric(metric, namespace, dimensions, value):
    client = boto3.client('cloudwatch', 'us-west-2')
    client.put_metric_data(
        Namespace=namespace,
        MetricData=[{
            'MetricName': metric,
            'Dimensions': dimensions,
            'Timestamp': datetime.datetime.utcnow(),
            'Value': value
        }]
    )


def metadata_record(ctx, metadata, table, e=None):
    if e is None and 'EXCEPTION' in metadata:
        e = metadata['EXCEPTION']
        del metadata['EXCEPTION']

    if e is not None:
        exception_only = format_exception_only(e)
        metadata['ERROR'] = {
            'EXCEPTION': format_exception(e),
            'EXCEPTION_ONLY': exception_only,
        }
        if exception_only.startswith('snowflake.connector.errors.ProgrammingError: '):
            metadata['ERROR']['PROGRAMMING_ERROR'] = exception_only[45:]

    metadata.setdefault('ROW_COUNT', {'INSERTED': 0, 'UPDATED': 0, 'SUPPRESSED': 0, 'PASSED': 0})

    metadata['END_TIME'] = datetime.datetime.utcnow()
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
        info(f"{record_type} metadata recorded.")

    except Exception as e:
        error(f"{record_type} metadata failed to log.", e)
