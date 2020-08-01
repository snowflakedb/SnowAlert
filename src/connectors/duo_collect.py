"""Duo Admins Inventory
Collect Duo Admin inventory using API keys
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
        'name': 'domainkey',
        'title': "DUO Account Code",
        'prompt': "In subdomain of your DUO account",
        'type': 'str',
        'postfix': ".duosecurity.com",
        'prefix': "api-",
        'placeholder': "47a8d864",
        'required': True,
    },
    {
        'name': 'skey',
        'title': "Secret Key",
        'prompt': "This secret is available in your Duo Admin Settings",
        'type': 'str',
        'secret': True,
        'required': True,
    },
    {
        'name': 'ikey',
        'title': "Integration Key",
        'prompt': "This secret is available in your Duo Admin Settings",
        'type': 'str',
        'secret': True,
        'required': True,
    },
]

LANDING_ADMIN_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('recorded_at', 'TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP'),
]


def connect(connection_name, options):
    table_name = f'duo_{connection_name}'
    landing_admin_table = f'data.{table_name}_admins_connection'

    db.create_table(
        name=landing_admin_table,
        cols=LANDING_ADMIN_TABLE_COLUMNS,
        comment=yaml_dump(module='duo_collect', **options),
        rw_role=ROLE,
    )

    return {
        'newStage': 'finalized',
        'newMessage': "Duo ingestion admin table created!",
    }


def ingest(table_name, options, dryrun=False):
    domainkey = options['domainkey']
    skey = options['skey']
    ikey = options['ikey']

    admin_api = duo_client.Admin(
        ikey=ikey, skey=skey, host=f'api-{domainkey}.duosecurity.com',
    )
    admins = list(admin_api.get_admins())
    db.insert(
        f'data.{table_name}', [{'raw': a} for a in admins], dryrun=dryrun,
    )
    return len(admins)


def main(table_name, domainkey, ikey, skey, dryrun=False):
    return ingest(
        table_name,
        {
            'domainkey': domainkey,
            'ikey': ikey,
            'skey': skey,
        },
        dryrun=dryrun,
    )


if __name__ == '__main__':
    fire(main)
