"""HackerOne Data
Collect HackerOne reports and payment transactions using an API key
"""
from datetime import datetime
from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE
from urllib.error import HTTPError
from .utils import yaml_dump

import requests 

PAGE_SIZE = 500

CONNECTION_OPTIONS = [
    {
        'name': 'api_token',
        'title': "API Token",
        'prompt': "Available in your H1 Settings",
        'type': 'str',
        'secret': True,
        'required': True,
    },
    {
        'name': 'api_identifier',
        'title': "API Identifier",
        'prompt': "Available in your H1 Settings",
        'type': 'str',
        'secret': True,
        'required': True,
    },
    {
        'name': 'program_name',
        'title': "Program Name",
        'prompt': "Your handle in Program Settings",
        'type': 'str',
        'required': True,
    },
    {
        'name': 'account_id',
        'title': "Account ID",
        'prompt': "https://api.hackerone.com/core-resources/#programs-get-your-programs",
        'type': 'str',
        'required': False,
    },  
]

LANDING_TABLE_COLUMNS_TRANSACTIONS = [
    ('timestamp', 'TIMESTAMP_LTZ(9)'),
    ('type', 'string'),
    ('activity_date', 'TIMESTAMP_LTZ(9)'),
    ('activity_description', 'string'),
    ('bounty_award', 'int'),
    ('bounty_fee', 'int'),
    ('debit_or_credit_amount', 'int'),
    ('balance', 'int'),
    ('id', 'number'),
    ('url', 'string'),
]

LANDING_TABLE_COLUMNS_REPORTS = [
    ('timestamp', 'TIMESTAMP_LTZ(9)'),
    ('id', 'number'), 
    ('type', 'string'), 
    ('title', 'string'), 
    ('state', 'string'), 
    ('created_at', 'TIMESTAMP_LTZ(9)'), 
    ('vulnerability_information', 'string'), 
    ('triaged_at', 'TIMESTAMP_LTZ(9)'), 
    ('closed_at', 'TIMESTAMP_LTZ(9)'), 
    ('first_program_activity_at', 'TIMESTAMP_LTZ(9)'),
    ('bounty_awarded_at', 'TIMESTAMP_LTZ(9)'),
    ('rating', 'string'), 
    ('name', 'string'),
    ('description', 'string'), 
    ('external_id', 'string'), 
    ('asset_identifier', 'string'), 
    ('awarded_amount', 'int'), 
    ('awarded_bonus_amount', 'int'),
]

#Perform the API call
def get_data(url: str, token: str, params: dict = {}) -> dict:
    headers: dict = {
        "Accept": "application/json"
    }

    api_identifier = options['api_identifier']

    try:
        log.debug(f"Preparing GET: url={url} with params={params}")
        resp = requests.get(
            url,
            auth=(api_identifier, api_token),
            params=params,
            headers=headers
        )
        resp.raise_for_status()
    except HTTPError as http_err:
        log.error(f"Error GET: url={url}")
        log.error(f"HTTP error occurred: {http_err}")
        raise
    log.debug(resp.status_code)
    return resp.json()

#Create 3 tables(main table, landing table, supplementary table)
def connect(connection_name, options)
    table_name = f'h1_{connection_name}'
    LANDING_TRANSACTIONS_TABLE = f'data.h1_collect_transactions_connection'
    LANDING_REPORTS_TABLE = f'data.h1_collect_reports_connection'

    db.create_table(
        name=LANDING_TRANSACTIONS_TABLE,
        cols=LANDING_TABLE_COLUMNS_TRANSACTIONS,
        comment=yaml_dump(module='h1_collect', **options),
        rw_role=ROLE,
    )
    db.execute(f'GRANT INSERT, SELECT ON {LANDING_TRANSACTIONS_TABLE} TO ROLE {SA_ROLE}')
    return {'newStage': 'finalized', 'newMessage': "HackerOne transactions ingestion table created!"}

    db.create_table(
        name=LANDING_REPORTS_TABLE,
        cols=LANDING_TABLE_COLUMNS_REPORTS,
        comment=yaml_dump(module='h1_collect', **options),
        rw_role=ROLE,
    )
    db.execute(f'GRANT INSERT, SELECT ON {LANDING_REPORTS_TABLE} TO ROLE {SA_ROLE}')
    return {'newStage': 'finalized', 'newMessage': "HackerOne reports ingestion table created!"}
    
def ingest(table_name, options):
    ingest_type = 'transaction' if table_name.endswith('_TRANSACTIONS_CONNECTION') else 'report'
    landing_table = f'data.{table_name}'

    timestamp = datetime.utcnow()
    api_token = options['api_token']
    account_id = options['account_id']
    program_name = options['program_name']
    
    if ingest_type == 'transaction' and account_id:
        try:
            transactions = get_data(
                f'https://api.hackerone.com/v1/programs/{account_id}/billing/transactions',
                api_token,
                params=None
            )
        except requests.exceptions.HTTPError as e:
            log.error(e)
        else:
            db.insert(
                landing_table,
                values=[
                    {
                        'timestamp': timestamp, 
                        'type': transaction.get('type',''),
                        'activity_date': transaction['attributes'].get('activity_date','') if 'attributes' in transaction else '', 
                        'activity_description': transaction['attributes'].get('activity_description','') if 'attributes' in transaction else '',
                        'bounty_award': transaction['attributes'].get('bounty_award','') if 'attributes' in transaction else '',
                        'bounty_fee': transaction['attributes'].get('bounty_fee','') if 'attributes' in transaction else '',
                        'debit_or_credit_amount': transaction['attributes'].get('debit_or_credit_amount','') if 'attributes' in transaction else '',
                        'balance': transaction['attributes'].get('balance','') if 'attributes' in transaction else '',
                        'id': transaction['relationships']['report']['data'].get('id','') if 'relationships' in transaction else '', 
                        'url': transaction['relationships']['report']['links'].get('self','') if 'relationships' in transaction else '',  
                    }
                    for transaction in transactions
                ]
            )

    else:
        try:
            reports = get_data(
                'https://api.hackerone.com/v1/reports',
                api_token,
                params={
                    'filter[program][]': program_name
                }
            )
        except requests.exceptions.HTTPError as e:
            log.error(e)
        else:
            db.insert(
                landing_table,
                values=[
                    {
                        'timestamp': timestamp,
                        'id': report.get('id',''),
                        'type': report.get('type',''),
                        'title': report['attributes'].get('title', '') if 'attributes' in report else '',
                        'state': report['attributes'].get('state', '') if 'attributes' in report else '',
                        'created_at': report['attributes'].get('created_at', '') if 'attributes' in report else '',
                        'vulnerability_information': report['attributes'].get('vulnerability_information', '') if 'attributes' in report else '',
                        'triaged_at': report['attributes'].get('triaged_at', '') if 'attributes' in report else '',
                        'closed_at': report['attributes'].get('closed_at', '') if 'attributes' in report else '',
                        'first_program_activity_at': report['attributes'].get('first_program_activity_at', '') if 'attributes' in report else '',
                        'bounty_awarded_at': report['attributes'].get('bounty_awarded_at', '') if 'attributes' in report else '',
                        'rating': report['relationships']['severity']['data']['attributes'].get('rating','') if 'relationships' in report else '', 
                        'name': report['relationships']['weakness']['data']['attributes'].get('name','') if 'relationships' in report else '', 
                        'description': report['relationships']['weakness']['data']['attributes'].get('description','') if 'relationships' in report else '',
                        'external_id': report['relationships']['weakness']['data']['attributes'].get('external_id','') if 'relationships' in report else '',
                        'asset_identifier': report['relationships']['structured_scope']['data']['attributes'].get('asset_identifier','') if 'relationships' in report else '',
                        'awarded_amount': report['relationships']['bounties']['data']['attributes'].get('awarded_amount','') if 'relationships' in report else '',
                        'awarded_bonus_amount': report['relationships']['bounties']['data']['attributes'].get('awarded_bonus_amount','') if 'relationships' in report else '',
                    }
                    for report in reports
                ]
            )