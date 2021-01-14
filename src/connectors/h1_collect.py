"""HackerOne Data
Collect HackerOne reports and payment transactions using an API key
"""
import os
from datetime import datetime
from email.utils import parsedate_to_datetime
from urllib.error import HTTPError

import fire
import requests
from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from connectors.utils import yaml_dump

PAGE_SIZE = 100

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
        'prompt': "Instructions to get account ID are found here: https://api.hackerone.com/core-resources/#programs-get-your-programs",
        'type': 'str',
        'required': False,
    },
]

LANDING_TABLE_COLUMNS_TRANSACTIONS = [
    ('recorded_at', 'TIMESTAMP_LTZ(9)'),
    ('type', 'string'),
    ('activity_date', 'TIMESTAMP_LTZ(9)'),
    ('activity_description', 'string'),
    ('bounty_award', 'float'),
    ('bounty_fee', 'float'),
    ('debit_or_credit_amount', 'float'),
    ('balance', 'float'),
    ('id', 'number'),
    ('url', 'string'),
]

LANDING_TABLE_COLUMNS_REPORTS = [
    ('recorded_at', 'TIMESTAMP_LTZ(9)'),
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
    ('awarded_amount', 'float'),
    ('awarded_bonus_amount', 'float'),
]


def load_data(
    url: str,
    token: str,
    api_identifier: str,
    params: dict = {}
) -> requests.Response:
    """Perform the API call"""
    headers: dict = {"Accept": "application/json"}

    try:
        log.debug(f"Preparing GET: url={url} with params={params}")
        resp = requests.get(
            url, auth=(api_identifier, token), params=params, headers=headers
        )
        resp.raise_for_status()
    except HTTPError as http_err:
        log.error(f"HTTP error occurred: {http_err}")
        raise
    log.debug(resp.status_code)
    return resp


def get_path(data: dict, path: str, default=None):
    for key in path.split("."):
        if key not in data:
            return default
        else:
            data = data[key]
    return data


def connect(connection_name, options):
    landing_transactions_table = f'data.h1_collect_{connection_name}_transactions_connection'
    landing_reports_table = f'data.h1_collect_{connection_name}_reports_connection'

    comment = yaml_dump(module='h1_collect', **options)

    db.create_table(
        name=landing_transactions_table,
        cols=LANDING_TABLE_COLUMNS_TRANSACTIONS,
        comment=comment,
        rw_role=SA_ROLE
    )

    db.create_table(
        name=landing_reports_table,
        cols=LANDING_TABLE_COLUMNS_REPORTS,
        comment=comment,
        rw_role=SA_ROLE
    )
    return {
        'newStage': 'finalized',
        'newMessage': "HackerOne ingestion tables created!",
    }


def insert_reports(landing_table, reports, recorded_at, dryrun):
    db.insert(
        landing_table,
        dryrun=dryrun,
        values=[
            {
                'recorded_at': recorded_at,
                'id': report.get('id', None),
                'type': report.get('type', None),
                'title': get_path(report, 'attributes.title'),
                'state': get_path(report, 'attributes.state'),
                'created_at': get_path(report, 'attributes.created_at'),
                'vulnerability_information': get_path(report, 'attributes.vulnerability_information'),
                'triaged_at': get_path(report, 'attributes.triaged_at'),
                'closed_at': get_path(report, 'attributes.closed_at'),
                'first_program_activity_at': get_path(report, 'attributes.first_program_activity_at'),
                'bounty_awarded_at': get_path(report, 'attributes.bounty_awarded_at'),
                'rating': get_path(report, 'relationships.severity.data.attributes.rating'),
                'name': get_path(report, 'relationships.weakness.data.attributes.name'),
                'description': get_path(report, 'relationships.weakness.data.attributes.description'),
                'external_id': get_path(report, 'relationships.weakness.data.attributes.external_id'),
                'asset_identifier': get_path(report, 'relationships.structured_scope.data.attributes.asset_identifier'),
                'awarded_amount': sum(
                    float(get_path(bounty, 'attributes.awarded_amount', 0.0))
                    for bounty in get_path(report, 'relationships.bounties.data', [])
                ),
                'awarded_bonus_amount': sum(
                    float(get_path(bounty, 'attributes.awarded_bonus_amount', 0.0))
                    for bounty in get_path(report, 'relationships.bounties.data', [])
                )
            }
            for report in reports
        ],
    )


def insert_transactions(landing_table, transactions, recorded_at, dryrun):
    db.insert(
        landing_table,
        dryrun=dryrun,
        values=[
            {
                'recorded_at': recorded_at,
                'type': transaction.get('type', ''),
                'activity_date': get_path(transaction, 'attributes.activity_date'),
                'activity_description': get_path(transaction, 'attributes.activity_description'),
                'bounty_award': get_path(transaction, 'attributes.bounty_award'),
                'bounty_fee': get_path(transaction, 'attributes.bounty_fee'),
                'debit_or_credit_amount': get_path(transaction, 'attributes.debit_or_credit_amount'),
                'balance': get_path(transaction, 'attributes.balance'),
                'id': get_path(transaction, 'relationships.report.data.id'),
                'url': get_path(transaction, 'relationships.report.links.self'),
            }
            for transaction in transactions
        ],
    )


def paginated_insert_reports(landing_table, options, dryrun):
    page_number = 1
    api_identifier = options['api_identifier']
    api_token = options['api_token']
    program_name = options['program_name']

    response = load_data(
        'https://api.hackerone.com/v1/reports',
        api_token,
        api_identifier,
        params={
            'filter[program][]': program_name,
            'page[size]': PAGE_SIZE,
            'page[number]': page_number
        },
    )
    reports = response.json()['data']
    recorded_at = parsedate_to_datetime(response.headers.get('Date'))
    # Insert 1st page
    insert_reports(landing_table, reports, recorded_at, dryrun)

    while 'next' in response.json()['links']:
        page_number += 1
        response = load_data(
            'https://api.hackerone.com/v1/reports',
            api_token,
            api_identifier,
            params={
            'filter[program][]': program_name,
            'page[size]': PAGE_SIZE,
            'page[number]': page_number
            },
        )   
        reports = response.json()['data']
        recorded_at = parsedate_to_datetime(response.headers.get('Date'))
        # Insert new page
        insert_reports(landing_table, reports, recorded_at, dryrun)


def paginated_insert_transactions(landing_table, options, dryrun):
    page_number = 1
    api_identifier = options['api_identifier']
    api_token = options['api_token']
    account_id = options['account_id']

    response = load_data(
        f'https://api.hackerone.com/v1/programs/{account_id}/billing/transactions',
        api_token,
        api_identifier,
        params={
            'page[size]': PAGE_SIZE,
            'page[number]': page_number
        },
    )
    transactions = response.json()['data']
    recorded_at = parsedate_to_datetime(response.headers.get('Date'))
    insert_transactions(landing_table, transactions, recorded_at, dryrun)

    while 'next' in response.json()['links']:
        page_number += 1
        response = load_data(
            f'https://api.hackerone.com/v1/programs/{account_id}/billing/transactions',
             api_token,
             api_identifier,
             params={
                 'page[size]': PAGE_SIZE,
                 'page[number]': page_number
             },
        )
    transactions = response.json()['data']
    recorded_at = parsedate_to_datetime(response.headers.get('Date'))
    insert_transactions(landing_table, transactions, recorded_at, dryrun)


def ingest(table_name, options, dryrun=False):
    ingest_type = (
        'transaction' if table_name.endswith('_TRANSACTIONS_CONNECTION') else 'report'
    )
    landing_table = f'data.{table_name}'

    if ingest_type == 'transaction' and options['account_id']:
        paginated_insert_transactions(landing_table, options, dryrun)   
    else:
        paginated_insert_reports(landing_table, options, dryrun)


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
