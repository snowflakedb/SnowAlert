"""SA Connections Runner (SAIR)

 SAIR processes Data Connections in *_CONNECTION tables

"""

import importlib
from datetime import datetime
from types import GeneratorType
from runners.helpers import log
from runners.helpers import db
from runners.helpers import vault
from runners.config import RUN_ID
from runners.config import DC_METADATA_TABLE

import yaml


def main(connection_table="%_CONNECTION"):
    for table in db.fetch(f"SHOW TABLES LIKE '{connection_table}' IN data"):
        table_name = table['name']
        table_comment = table['comment']

        log.info(f"-- START DC {table_name} --")
        try:
            options = yaml.load(table_comment) or {}

            if 'module' in options:
                module = options['module']

                metadata = {
                    'RUN_ID': RUN_ID,
                    'TYPE': module,
                    'START_TIME': datetime.utcnow(),
                    'LANDING_TABLE': table_name,
                    'INGEST_COUNT': 0
                }

                connector = importlib.import_module(f"connectors.{module}")

                for module_option in connector.CONNECTION_OPTIONS:
                    name = module_option['name']
                    if module_option.get('secret') and name in options:
                        options[name] = vault.decrypt_if_encrypted(options[name])

                if callable(getattr(connector, 'ingest', None)):
                    ingested = connector.ingest(table_name, options)
                    if isinstance(ingested, int):
                        metadata['INGEST_COUNT'] += ingested
                    elif isinstance(ingested, GeneratorType):
                        for n in ingested:
                            metadata['INGEST_COUNT'] += n
                    else:
                        metadata['INGESTED'] = ingested

                db.record_metadata(metadata, table=DC_METADATA_TABLE)

        except Exception as e:
            log.error(f"Error loading logs into {table_name}: ", e)
            db.record_metadata(metadata, table=DC_METADATA_TABLE, e=e)

        log.info(f"-- END DC --")


if __name__ == "__main__":
    main()
