from typing import List, Dict, Any

from runners.config import (
    DATA_SCHEMA,
)

from runners.helpers import db, log

from rpy2 import robjects as ro
from rpy2.robjects import pandas2ri

import pandas
import yaml


def format_code(code, vars):
    for k, v in vars.items():
        code = code.replace(k, v)
    return code


def pack(data: List[Dict[Any, Any]]) -> Dict[Any, List[Any]]:
    keys = {k for row in data for k in row.keys()}
    return {k: [d.get(k) for d in data] for k in keys}


def unpack(data):
    b = [[data[k][i] for i in data[k]] for k in data]
    return list(zip(*b))


def query_log_source(source, time_filter, time_column):
    cutoff = f"DATEADD(day, -{time_filter}, CURRENT_TIMESTAMP())"
    query = f"SELECT * FROM {source} WHERE {time_column} > {cutoff};"
    try:
        data = db.fetch(query)
    except Exception as e:
        log.error("Failed to query log source: ", e)

    f = pack(data)
    frame = pandas.DataFrame(f)
    pandas2ri.activate()
    r_dataframe = pandas2ri.py2rpy(frame)
    return r_dataframe


def run_baseline(name, comment):
    try:
        metadata = yaml.load(comment)
        assert type(metadata) is dict

        source = metadata['log source']
        required_values = metadata['required values']
        code_location = metadata['module name']
        time_filter = metadata['filter']
        time_column = metadata['history']

    except Exception as e:
        log.error(e, "{name} has invalid metadata, skipping")
        return

    with open(f"../baseline_modules/{code_location}/{code_location}.R") as f:
        r_code = f.read()

    r_code = format_code(r_code, required_values)
    frame = query_log_source(source, time_filter, time_column)
    ro.globalenv['input_table'] = frame

    output = ro.r(r_code)
    output = output.to_dict()

    results = unpack(output)
    try:
        db.insert(f"{DATA_SCHEMA}.{name}", results, ovewrite=True)
    except Exception as e:
        log.error("Failed to insert the results into the target table", e)


def main():
    db.connect()
    for table in db.fetch(f"show tables like '%_BASELINE' in {DATA_SCHEMA}"):
        name = table['name']
        comment = table['comment']
        log.info(f'{name} started...')
        run_baseline(name, comment)
        log.info(f'{name} done.')
