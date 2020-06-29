from os import environ as env

import requests

from runners.helpers import log
from runners.helpers import vault


class Bearer(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


def handle(alert, assignee=''):
    host = env.get('SA_SN_API_HOST')
    if not host:
        log.info('skipping service-now handler, missing host')

    client_id = env.get('SA_SN_OAUTH_CLIENT_ID')
    username = vault.decrypt_if_encrypted(envar='SA_SN_API_USER')
    password = vault.decrypt_if_encrypted(envar='SA_SN_API_PASS')

    client_secret = vault.decrypt_if_encrypted(envar='SA_SN_OAUTH_CLIENT_SECRET')
    refresh_token = vault.decrypt_if_encrypted(envar='SA_SN_OAUTH_REFRESH_TOKEN')

    if client_id:
        oauth_return_params = {
            'grant_type': 'refresh_token',
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token,
        }

        oauthresp = requests.post(
            'https://snowflakedev.service-now.com/oauth_token.do',
            data=oauth_return_params,
        )

        result = oauthresp.json()
        access_token = result.get('access_token')

        if not access_token:
            log.info('skipping service-now handler, bad oauth')
            raise RuntimeError(result)
    else:
        access_token = None

    if not (username and password) and not access_token:
        return

    endpoint = f'https://{host}/api/now/table/incident'
    title = alert.get('TITLE', 'SnowAlert Generate Incident')

    response = requests.post(
        endpoint,
        auth=Bearer(access_token) if access_token else (username, password),
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
