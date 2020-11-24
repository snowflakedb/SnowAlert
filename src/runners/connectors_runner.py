"""SA Connections Runner (SAIR)

 SAIR processes Data Connections in *_CONNECTION tables

"""
import fire

from multiprocessing import Pool
from datetime import datetime
import importlib
import json
import re
from types import GeneratorType
import yaml

from runners.helpers import db, log, vault
from runners.config import RUN_ID, DC_METADATA_TABLE, DC_POOLSIZE


def do_ingest(connector, table_name, options):
    ingested = connector.ingest(table_name, options)
    return (
        sum(ingested)
        if isinstance(ingested, GeneratorType)
        else ingested
        if isinstance(ingested, int)
        else None
    )


def time_to_run(schedule, now) -> bool:
    """assuming 15-min runner task, checks whether scheduled DC should run

    todo(anf): robust cron using prior run metadata
    """
    if schedule == '0 1-13/12':  # every 12 hours offset by 1
        if now.minute < 15 and now.hour % 12 == 1:
            return True

    m = re.match(r'^0 \*/([0-9]+)$', schedule)
    if m:  # every N hours
        n = int(m.group(1))
        if now.minute < 15 and now.hour % n == 0:
            return True

    if schedule == '0 *':  # hourly
        if now.minute < 15:
            return True

    return False


def connection_run(connection_table, run_now=False, option_overrides={}):
    table_name = connection_table['name']
    table_comment = connection_table['comment']

    log.info(f"-- START DC {table_name} --")
    try:
        metadata = {'START_TIME': datetime.utcnow()}
        options = yaml.safe_load(table_comment) or {}
        options.update(option_overrides)

        if 'schedule' in options:
            schedule = options['schedule']
            now = datetime.now()
            if not run_now and not time_to_run(schedule, now):
                log.info(f'not scheduled: {schedule} at {now}')
                log.info(f"-- END DC --")
                return

        if 'module' not in options:
            log.info(f'no module in options')
            log.info(f"-- END DC --")
            return

        module = options['module']

        metadata.update(
            {'RUN_ID': RUN_ID, 'TYPE': module, 'LANDING_TABLE': table_name}
        )

        connector = importlib.import_module(f"connectors.{module}")

        for module_option in connector.CONNECTION_OPTIONS:
            name = module_option['name']
            options.setdefault(name, module_option.get('default'))

            if module_option.get('secret') and name in options:
                options[name] = vault.decrypt_if_encrypted(options[name])
            if module_option.get('type') == 'json':
                options[name] = json.loads(options[name])
            if module_option.get('type') == 'list':
                if type(options[name]) is str:
                    options[name] = options[name].split(',')
            if module_option.get('type') == 'int':
                options[name] = int(options[name])

        if callable(getattr(connector, 'ingest', None)):
            db.record_metadata(metadata, table=DC_METADATA_TABLE)
            result = do_ingest(connector, table_name, options)
            if result is not None:
                metadata['INGEST_COUNT'] = result
            else:
                metadata['INGESTED'] = result

        db.record_metadata(metadata, table=DC_METADATA_TABLE)

    except Exception as e:
        log.error(f"Error loading logs into {table_name}: ", e)
        db.record_metadata(metadata, table=DC_METADATA_TABLE, e=e)

    log.info(f"-- END DC --")


def main(connection_table=None, run_now=False, **option_overrides):
    if connection_table is not None:
        # for a single table, we ignore schedule and run now
        run_now = True
    else:
        connection_table = "%_CONNECTION"

    tables = list(db.fetch(f"SHOW TABLES LIKE '{connection_table}' IN data"))
    if len(tables) == 1:
        connection_run(tables[0], run_now=run_now, option_overrides=option_overrides)
    else:
        Pool(DC_POOLSIZE).map(connection_run, tables)


if __name__ == "__main__":
    fire.Fire(main)
