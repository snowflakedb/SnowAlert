from os import environ as env

import requests

from runners.helpers import log
from runners.helpers import vault


def handle(alert, assignee=''):
    host = env.get('SA_SN_API_HOST')
    username = vault.decrypt_if_encrypted(envar='SA_SN_API_USER')
    password = vault.decrypt_if_encrypted(envar='SA_SN_API_PASS')

    if not host:
        log.info('skipping service-now handler, missing host')
    if not username:
        log.info('skipping service-now handler, missing username')
    if not password:
        log.info('skipping service-now handler, missing password')

    if not (host and username and password):
        return

    endpoint = f'https://{host}.service-now.com/api/now/table/incident'
    title = alert.get('TITLE', 'SnowAlert Generate Incident')

    response = requests.post(
        endpoint,
        auth=(username, password),
        data={
            'contact_type': 'Integration',
            'impact': '2',
            'urgency': '2',
            'category': 'IT Security',
            'subcategory': 'Remediation',
            'short_description': title,
            'assigned_to': assignee,
        },
    )

    if response.status_code != 201:
        log.info(
            f'Status: {response.status_code}',
            f'Headers: {response.headers}',
            f'Error Response: {response.json()}',
        )
        raise RuntimeError(response)

    return response
