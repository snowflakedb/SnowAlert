"""Duo Admins
Collect Duo admins using a key 
"""

from datetime import datetime
from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE

import duo_client
import fire

from .utils import yaml_dump

PAGE_SIZE = 500

CONNECTION_OPTIONS = [
    {
        'name': 'subdomain',
        'title': "DUO Account Name",
        'prompt': "The Subdomain of your DUO",
        'type': 'str',
        'postfix': ".duosecurity.com",
        'prefix': "api-",
        'placeholder': "account-name",
        'required': True,
    },
    {
        'name': 'duo_key',
        'title': "Secret Key",
        'prompt': "This available in your Duo Admin Settings ",
        'type': 'str',
        'secret': True,
        'required': True,
    },
    {
        'name': 'duo_integration_key',
        'title': "Integration Key",
        'prompt': "This available in your Duo Admin Settings ",
        'type': 'str',
        'secret': True,
        'required': True,
    },
]

LANDING_ADMIN_TABLE_COLUMNS = [('raw', 'VARIANT', 'RECORDED_AT', 'TIMESTAMP_LTZ')]


def connect(connection_name, options):
    table_name = f'duo_{connection_name}'
    landing_admin_table = f'data.{table_name}_admins_connection'

    db.create_table(
        name=landing_admin_table,
        cols=LANDING_ADMIN_TABLE_COLUMNS,
        comment=yaml_dump(module='duo', **options),
        rw_role=ROLE,
    )

    return {
        'newStage': 'finalized',
        'newMessage': "Duo ingestion admin table created!",
    }


def ingest(table_name, options, dryrun=False):
    landing_table = f'data.{table_name}'
    timestamp = datetime.utcnow()

    url = options['subdomain']
    integration_key = options['duo_integration_key']
    token = options['duo_key']
    admin_api = duo_client.Admin(ikey=integration_key, skey=token, host=url)

    offset = 0
    while True:
        admins = admin_api.get_admins(limit=PAGE_SIZE, offset=offset)
        len_admins = len(admins)
        if len_admins == 0:
            break

        db.insert(
            landing_table,
            [{'raw': admin, 'recorded_at': timestamp} for admin in admins],
            dryrun=dryrun
        )

        log.info(f'Inserted {len_admins} rows.')
        offset += len_admins
        yield len_admins


def main(table_name, subdomain, duo_integration_key, duo_key, dryrun=False):
    return ingest(
        table_name,
        {
            'subdomain': subdomain,
            'duo_integration_key': duo_integration_key,
            'duo_key': duo_key,
        },
        dryrun=dryrun,
    )


if __name__ == '__main__':
    fire(main)
