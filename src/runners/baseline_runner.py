from runners.config import (
    DATA_SCHEMA_NAME,
)

from runners.helpers import db, log

from rpy2 import robjects as ro
from rpy2.robjects import pandas2ri

import pandas
import yaml


def read_metadata(ctx):
    query = f"""show tables like '%_BASELINE' in snowalert.{DATA_SCHEMA_NAME}"""
    try:
        rows = db.fetch(ctx, query)
        return list(rows)
    except Exception as e:
        log.error("Unable to read metadata: ", e)
        return None


def format_code(r, vars):
    ret = r
    for k, v in vars.items():
        ret = ret.replace(k, v)

    return ret


def pack(data):
    f = {}
    for row in data:
        if len(f) == 0:
            for k in row.keys():
                f.update({k: []})

        for k in row:
            f[k].append(row[k])

    return f


def unpack(data):
    b = [[data[k][i] for i in data[k]] for k in data]
    output = list(zip(*b))
    return output


def query_log_source(ctx, source, time_filter, history):
    log.info("Querying log source and packing data into frame")
    query = f"""SELECT * from {source} where {history} > dateadd(day, -{time_filter}, current_timestamp());"""
    if ctx is not None:
        try:
            data = db.fetch(ctx, query)
        except Exception as e:
            log.error("Failed to query log source: ", e)
    else:
        log.error("No connection passed in for log source query.")
        return None

    f = pack(data)

    frame = pandas.DataFrame(f)
    pandas2ri.activate()
    r_dataframe = pandas2ri.py2rpy(frame)
    return r_dataframe


def log_results_query(target, results):
    query = f"""insert into snowalert.{DATA_SCHEMA_NAME}.{target} values """
    for i in results:
        query += str(i) + ", "

    query = query[:-2]
    return query


def run_baseline(ctx, row):
    try:
        metadata = yaml.load(row['comment'])
    except Exception:
        log.error("Failed to load metadata from comment")
        return None

    if metadata is None:
        log.error(f"No comment for table {row['name']}")
        return None

    log_source = metadata['log source']
    required_values = metadata['required values']
    output_table = row['name']
    code_location = metadata['module name']
    time_filter = metadata['filter']
    history = metadata['history']

    log.info("Reading R code")

    with open(f"../baseline_modules/{code_location}/{code_location}.R", "r") as f:
        r_code = f.read()

    r_code = format_code(r_code, required_values)
    frame = query_log_source(ctx, log_source, time_filter, history)
    ro.globalenv['input_table'] = frame

    log.info("Running R code")
    output = ro.r(r_code)
    print(type(output))
    output = output.to_dict()

    results = unpack(output)
    try:
        db.execute(ctx, f'truncate table snowalert.{DATA_SCHEMA_NAME}.{output_table};')
        db.execute(ctx, log_results_query(output_table, results))
    except Exception as e:
        log.error("Failed to insert the results into the target table", e)


def main():
    ctx = db.connect()
    log.info("Finding baseline metadata")
    tables = read_metadata(ctx)

    for row in tables:
        log.info(f"Starting baseline: {row['name']}")
        run_baseline(ctx, row)
