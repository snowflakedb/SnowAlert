from runners.helpers import log, db, kms
import os
import subprocess
import yaml
import importlib


#  The pipeline runner needs to list all the files in the pipeline folder and then invoke them one by one.

# The pipeline runner also has to iterate through all the connection table and call ingest() on them.

CONNECTION_TABLE_QUERY = f"""
SHOW TABLES LIKE '%_CONNECTION' in DATA
"""


def main(connection_table=""):
    for name in os.listdir('../ingestion'):
        log.info(f"invoking {name}")
        try:
            res = subprocess.call(f"python ../ingestion/{name}", shell=True)
            log.info("subprocess returns: ", res)
            log.info(f"{name} invoked")
        except Exception as e:
            log.error(f"failed to run {name}", e)

    if connection_table:
        tables = db.fetch(f"SHOW TABLES LIKE '{connection_table}' IN DATA")
    else:
        tables = db.fetch(CONNECTION_TABLE_QUERY)

    for table in tables:
        log.info(f"Starting {table['name']}")
        options = yaml.load(table['comment'])
        if 'module' in options:
            connection_module = importlib.import_module(f"connectors.{options['module']}")
            for option in options:
                if connection_module.CONNECTION_OPTIONS.get(option, {}).get('secret'):
                    options[option] = kms.decrypt_if_encrypted(options[option])
            connection_module.ingest(options['name'], options)


if __name__ == "__main__":
    main()
