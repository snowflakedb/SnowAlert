"""Duo Admins
Collect Duo admins using a key 
"""

 from runners.helpers import db, log
 from runners.helpers.dbconfig import ROLE as SA_ROLE
 from .utils import yaml_dump

from datetime import datetime
import duo_client

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
    }
]

LANDING_ADMIN_TABLE_COLUMNS = [('raw', 'VARIANT', 'RECORDED_AT', 'TIMESTAMP_LTZ' )]

def connect(connection_name, options):
    table_name=f'duo_{connection_name}'
    landing_admin_table=f'data.{table_name}_admins_connection'

    comment = yaml_dump(module='duo', **options)

    db.create_table(
        name=landing_admin_table, cols=LANDING_ADMIN_TABLE_COLUMNS, comment=comment
    )
    db.execute(f'GRANT INSERT, SELECT  ON {landing_admin_table} TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': "Duo ingestion admin table created!",
    }

def authenticate_duo(URL, integration_key, token):
    admin_api = duo_client.Admin(
        ikey=integration_key,
        skey=token,
        host=URL,
    )
    return(admin_api)
    

def ingest(table_name, options): 
    landing_table = f'data.{table_name}'
    timestamp = datetime.utcnow()

    URL = options['subdomain']
    integration_key = options['duo_integration_key']
    token = options['duo_key']
    admin_api = authenticate_duo(URL, integration_key, token)


    offset = 0
    while True:
        admins = admin_api.get_admins(limit=PAGE_SIZE, offset=offset)
        len_admins = len(admins)
        if (len_admins==0):
            break

        db.insert(
            landing_table,
            values=[(admin, timestamp) for admin in admins],
            select='PARSE_JSON(column1), column2',
        )

        log.info(f'Inserted {len_admins} rows.')
        offset += len_admins
        yield len_admins
