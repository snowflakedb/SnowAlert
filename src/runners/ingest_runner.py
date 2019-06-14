from runners.helpers import log, db, kms
import os
import subprocess
import yaml
import importlib


#  The pipeline runner needs to list all the files in the pipeline folder and then invoke them one by one.

# The pipeline runner also has to iterate through all the connection table and call ingest() on them.

def main(connection_table="%_CONNECTION"):
    # for name in os.listdir('../ingestion'):
    #     log.info(f"invoking {name}")
    #     try:
    #         res = subprocess.call(f"python ../ingestion/{name}", shell=True)
    #         log.info("subprocess returns: ", res)
    #         log.info(f"{name} invoked")
    #     except Exception as e:
    #         log.error(f"failed to run {name}", e)

    tables = db.fetch(f"SHOW TABLES LIKE '{connection_table}' IN data")

    for table in tables:
        log.info(f"Starting {table['name']}")
        options = yaml.load(table['comment'])
        if 'module' in options:
            connector = importlib.import_module(f"connectors.{options['module']}")
            for option in options:
                for module_option in connector.CONNECTION_OPTIONS:
                    if module_option['name'] == option and module_option.get('secret'):
                        options[option] = kms.decrypt_if_encrypted(options[option])
            connector.ingest(table['name'], options)


if __name__ == "__main__":
    main()
