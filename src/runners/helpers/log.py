import datetime
import sys
import traceback
from runners import utils
from os.path import relpath
from os import getpid

import boto3

from ..config import ENV
from .exception_tracker import ExceptionTracker

EXCEPTION_TRACKER = ExceptionTracker()


def write(*args, stream=sys.stdout):
    for a in args:
        if isinstance(a, Exception):

            def fmt(fs):
                return (
                    './'
                    + relpath(fs.filename)
                    + f':{fs.lineno}'
                    + f' in {fs.name}\n'
                    + f'    {fs.line}\n'
                )

            trace = traceback.extract_tb(a.__traceback__)
            fmt_trace = ''.join(fmt(f) for f in trace)
            stack = traceback.extract_stack()
            for i, f in enumerate(reversed(stack)):
                if (f.filename, f.name) == (trace[0].filename, trace[0].name):
                    stack = stack[:-i]
                    break  # skip the log.py part of stack
            for i, f in enumerate(reversed(stack)):
                if 'site-packages' in f.filename:
                    stack = stack[-i:]
                    break  # skip the flask part of stack
            fmt_stack = ''.join(fmt(f) for f in stack)

            a = (
                fmt_stack
                + '--- printed exception w/ trace ---\n'
                + fmt_trace
                + utils.format_exception_only(a)
            )

        pid = getpid()
        print(f'[{pid}] {a}', file=stream, flush=True)


def debug(*args):
    if ENV in ('dev', 'test'):
        write(*args, stream=sys.stdout)


def info(*args):
    write(*args, stream=sys.stdout)


def error(*args):
    EXCEPTION_TRACKER.notify(*args)
    write(*args, stream=sys.stderr)


def fatal(*args):
    error(*args)
    sys.exit(1)


def metric(metric, namespace, dimensions, value):
    client = boto3.client('cloudwatch', 'us-west-2')
    client.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {
                'MetricName': metric,
                'Dimensions': dimensions,
                'Timestamp': datetime.datetime.utcnow(),
                'Value': value,
            }
        ],
    )
