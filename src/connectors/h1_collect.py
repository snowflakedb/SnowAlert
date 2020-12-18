"""HackerOne Data
Collect HackerOne reports and payment transactions using an API key
"""
import os
from datetime import datetime
from urllib.error import HTTPError

import fire
import requests
from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from connectors.utils import yaml_dump

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

# Perform the API call
def get_data(url: str, token: str, api_identifier: str, params: dict = {}) -> dict:
    headers: dict = {"Accept": "application/json"}

    try:
        log.debug(f"Preparing GET: url={url} with params={params}")
        resp = requests.get(
            url, auth=(api_identifier, token), params=params, headers=headers
        )
        resp.raise_for_status()
    except HTTPError as http_err:
        log.error(f"Error GET: url={url}")
        log.error(f"HTTP error occurred: {http_err}")
        raise
    log.debug(resp.status_code)
    return resp.json()['data']


def connect(connection_name, options):
    landing_transactions_table = f'data.h1_collect_transactions_connection'
    landing_reports_table = f'data.h1_collect_reports_connection'

    comment = yaml_dump(module='h1_collect', **options)

    db.create_table(
        name=landing_transactions_table,
        cols=LANDING_TABLE_COLUMNS_TRANSACTIONS,
        comment=comment,
    )
    db.execute(
        f'GRANT INSERT, SELECT ON {landing_transactions_table} TO ROLE {SA_ROLE}'
    )

    db.create_table(
        name=landing_reports_table,
        cols=LANDING_TABLE_COLUMNS_REPORTS,
        comment=comment,
    )
    db.execute(f'GRANT INSERT, SELECT ON {landing_reports_table} TO ROLE {SA_ROLE}')
    return {
        'newStage': 'finalized',
        'newMessage': "HackerOne reports ingestion table created!",
    }


def ingest(table_name, options, dryrun=False):
    ingest_type = (
        'transaction' if table_name.endswith('_TRANSACTIONS_CONNECTION') else 'report'
    )
    landing_table = f'data.{table_name}'

    timestamp = datetime.utcnow()
    api_identifier = options['api_identifier']
    api_token = options['api_token']
    account_id = options['account_id']
    program_name = options['program_name']

    if ingest_type == 'transaction' and account_id:
        try:
            transactions = get_data(
                f'https://api.hackerone.com/v1/programs/{account_id}/billing/transactions',
                api_token,
                api_identifier,
                params=None,
            )
        except requests.exceptions.HTTPError as e:
            log.error(e)
        else:
            db.insert(
                landing_table,
                dryrun=dryrun,
                values=[
                    {
                        'timestamp': timestamp,
                        'type': transaction.get('type', ''),
                        'activity_date': transaction['attributes'].get(
                            'activity_date', ''
                        )
                        if 'attributes' in transaction
                        else '',
                        'activity_description': transaction['attributes'].get(
                            'activity_description', ''
                        )
                        if 'attributes' in transaction
                        else '',
                        'bounty_award': transaction['attributes'].get(
                            'bounty_award', ''
                        )
                        if 'attributes' in transaction
                        else '',
                        'bounty_fee': transaction['attributes'].get('bounty_fee', '')
                        if 'attributes' in transaction
                        else '',
                        'debit_or_credit_amount': transaction['attributes'].get(
                            'debit_or_credit_amount', ''
                        )
                        if 'attributes' in transaction
                        else '',
                        'balance': transaction['attributes'].get('balance', '')
                        if 'attributes' in transaction
                        else '',
                        'id': transaction['relationships']['report']['data'].get(
                            'id', ''
                        )
                        if 'relationships' in transaction
                        else '',
                        'url': transaction['relationships']['report']['links'].get(
                            'self', ''
                        )
                        if 'relationships' in transaction
                        else '',
                    }
                    for transaction in transactions
                ],
            )
    else:
        try:
            reports = get_data(
                'https://api.hackerone.com/v1/reports',
                api_token,
                api_identifier,
                params={'filter[program][]': program_name},
            )
        except requests.exceptions.HTTPError as e:
            log.error(e)
        else:
            print(reports)
            db.insert(
                landing_table,
                dryrun=dryrun,
                values=[
                    {
                        'timestamp': timestamp,
                        'id': report.get('id', ''),
                        'type': report.get('type', ''),
                        'title': report['attributes'].get('title', '')
                        if 'attributes' in report
                        else '',
                        'state': report['attributes'].get('state', '')
                        if 'attributes' in report
                        else '',
                        'created_at': report['attributes'].get('created_at', '')
                        if 'attributes' in report
                        else '',
                        'vulnerability_information': report['attributes'].get(
                            'vulnerability_information', ''
                        )
                        if 'attributes' in report
                        else '',
                        'triaged_at': report['attributes'].get('triaged_at', '')
                        if 'attributes' in report
                        else '',
                        'closed_at': report['attributes'].get('closed_at', '')
                        if 'attributes' in report
                        else '',
                        'first_program_activity_at': report['attributes'].get(
                            'first_program_activity_at', ''
                        )
                        if 'attributes' in report
                        else '',
                        'bounty_awarded_at': report['attributes'].get(
                            'bounty_awarded_at', ''
                        )
                        if 'attributes' in report
                        else '',
                        'rating': report['relationships']['severity']['data'][
                            'attributes'
                        ].get('rating', '')
                        if 'relationships' in report and 'severity' in report['relationships'] 
                        else '',
                        'name': report['relationships']['weakness']['data'][
                            'attributes'
                        ].get('name', '')
                        if 'relationships' in report
                        else '',
                        'description': report['relationships']['weakness']['data'][
                            'attributes'
                        ].get('description', '')
                        if 'relationships' in report
                        else '',
                        'external_id': report['relationships']['weakness']['data'][
                            'attributes'
                        ].get('external_id', '')
                        if 'relationships' in report
                        else '',
                        'asset_identifier': report['relationships']['structured_scope'][
                            'data'
                        ]['attributes'].get('asset_identifier', '')
                        if 'relationships' in report and 'structured_scope' in report['relationships']
                        else '',
                        'awarded_amount': sum([
                            float(bounty['attributes'].get('awarded_amount', 0.0))
                            for bounty in report['relationships']['bounties']['data']
                        ])
                        if 'relationships' in report and 'bounties' in report['relationships'] and report['relationships']['bounties']['data']
                        else 0,
                        'awarded_bonus_amount': sum([
                            float(bounty['attributes'].get('awarded_amount', 0.0))
                            for bounty in report['relationships']['bounties']['data']
                        ])
                        if 'relationships' in report and 'bounties' in report['relationships'] and report['relationships']['bounties']['data']
                        else 0,
                    }
                    for report in reports
                ],
            )


def main(
    table_name='h1_collect_reports_connection',
    api_token=os.environ.get('H1_API_TOKEN'),
    api_identifier=os.environ.get('H1_API_IDENTIFIER'),
    program_name=os.environ.get('H1_PROGRAM_NAME'),
    account_id=os.environ.get('H1_ACCOUNT_ID'),
    dryrun=True,
):
    return ingest(
        table_name,
        {
            'api_token': api_token,
            'api_identifier': api_identifier,
            'program_name': program_name,
            'account_id': account_id,
        },
        dryrun=dryrun,
    )


if __name__ == '__main__':
    fire.Fire(main)
