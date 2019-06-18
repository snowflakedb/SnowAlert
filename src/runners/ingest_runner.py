"""SA Ingestion Runner (SAIR)

1. SAIR processes all the files in the src/ingestion folder
2. SAIR processes Data Connections in *_CONNECTION tables

"""
import importlib
import os
import subprocess
import yaml

from runners.helpers import log, db, kms


def main(connection_table="%_CONNECTION"):
    # ingestion scripts
    for name in os.listdir('../ingestion'):
        log.info(f"invoking {name}")
        try:
            res = subprocess.call(f"python ../ingestion/{name}", shell=True)
            log.info("subprocess returns: ", res)
            log.info(f"{name} invoked")
        except Exception as e:
            log.error(f"failed to run {name}", e)

    # data connections
    for table in db.fetch(f"SHOW TABLES LIKE '{connection_table}' IN data"):
        log.info(f"Starting {table['name']}")
        try:
            options = yaml.load(table['comment'])
            if 'module' in options:
                connector = importlib.import_module(f"connectors.{options['module']}")

                for option in options:
                    for module_option in connector.CONNECTION_OPTIONS:
                        if module_option['name'] == option and module_option.get('secret'):
                            options[option] = kms.decrypt_if_encrypted(options[option])

                if callable(getattr(connector, 'ingest', None)):
                    connector.ingest(table['name'], options)

        except Exception as e:
            log.error(e)

        log.info(f"Finished {table['name']}")


if __name__ == "__main__":
    main()
