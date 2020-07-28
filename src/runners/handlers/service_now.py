"""ServiceNow dispatcher

envars:
- config
  - SA_SN_API_HOST
  - SA_SN_API_ENDPOINT
  - SA_SN_FIELD_PREFIX

- auth1
  - SA_SN_API_USER
  - SA_SN_API_PASS

- auth2
  - SA_SN_OAUTH_CLIENT_ID
  - SA_SN_OAUTH_CLIENT_SECRET
  - SA_SN_OAUTH_REFRESH_TOKEN

"""

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
        return

    username = vault.decrypt_if_encrypted(envar='SA_SN_API_USER')
    password = vault.decrypt_if_encrypted(envar='SA_SN_API_PASS')

    client_id = env.get('SA_SN_OAUTH_CLIENT_ID')
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
            f'https://{host}/oauth_token.do',
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
        log.info('skipping service-now handler, no authorization')
        return

    title = alert.get('TITLE', 'SnowAlert Generate Incident')
    description = alert.get('DESCRIPTION', '')

    endpoint = env.get('SA_SN_API_ENDPOINT', '/now/table/incident')
    api_url = f'https://{host}/api{endpoint}'

    fp = env.get('SA_SN_FIELD_PREFIX', '')
    response = requests.post(
        api_url,
        auth=Bearer(access_token) if access_token else (username, password),
        json={
            f'{fp}contact_type': 'Integration',
            f'{fp}impact': '2',
            f'{fp}urgency': '2',
            f'{fp}category': 'IT Security',
            f'{fp}subcategory': 'Remediation',
            f'{fp}assignment_group': 'Security Compliance',
            f'{fp}short_description': title,
            f'{fp}description': description,
            f'{fp}assigned_to': assignee,
        },
    )

    if response.status_code != 201:
        log.info(
            f'URL: {api_url}',
            f'Status Code: {response.status_code}',
            f'Response Length: {len(response.text)}',
            f'Response Headers: {response.headers}',
        )
        raise RuntimeError(response)

    return response
