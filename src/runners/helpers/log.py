import traceback
import sys
import boto3
import datetime
import json


def write(*args, stream=sys.stdout):
    for a in args:
        if isinstance(a, Exception):
            traceback.print_exception(type(a), a, a.__traceback__, file=stream)
            stream.flush()
        else:
            print(a, file=stream, flush=True)


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
    metadata['EXCEPTION'] = ''.join(traceback.format_exception(type(e), e, e.__traceback__)) if e else None
    metadata['END_TIME'] = datetime.datetime.utcnow()
    metadata['DURATION'] = str(metadata['END_TIME'] - metadata['START_TIME'])
    metadata['ROW_COUNT'] = ctx.cursor().rowcount or 0

    metadata['START_TIME'] = str(metadata['START_TIME'])
    metadata['END_TIME'] = str(metadata['END_TIME'])

    statement = f'''
    INSERT INTO {table}
        (event_time, v) select '{metadata['START_TIME']}',
        PARSE_JSON(column1) from values('{json.dumps(metadata)}')
    '''

    try:
        info("Recording metadata.")
        ctx.cursor().execute(statement)
    except Exception as e:
        fatal("Metadata failed to log", e)
