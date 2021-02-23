"""Google Collect
Collect Google API responses using a Service Account
"""

from collections import namedtuple

from googleapiclient.discovery import build
from google.oauth2 import service_account

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE

from .utils import yaml_dump

CONNECTION_OPTIONS = [
    {
        'name': 'service_user_creds',
        'type': 'json',
        'title': "Service User Credentials",
        'prompt': "JSON Credentials from the Service User",
        'placeholder': (
            """{"type": "service_account", "project_id": "my-gcp-project", """
            """"private_key_id": "34d2d9933e3f4a9ee55b968758e5ca3c2b348221", """
            """"private_key": "-----BEGIN PRIVATE KEY-----\n...-----END PRIVATE KEY-----\n", """
            """... }"""
        ),
        'required': True,
        'secret': True,
    },
    {
        'type': 'list',
        'name': 'subjects_list',
        'title': "List of Subjects (optional)",
        'prompt': "Comma-separated list credentials delegated to Service Account",
        'placeholder': "auditor@first-gcp-project.company.com,auditor@second-gcp-project.company.com",
    },
]

column = namedtuple('column', ['name', 'type'])

API_SPECS = {
    'users': {
        'service_name': 'admin',
        'service_version': 'directory_v1',
        'scopes': ['https://www.googleapis.com/auth/admin.directory.user.readonly'],
        'resource_name': 'users',
        'method': 'list',
        'params': lambda subject: {'domain': subject.split('@')[1]},
    },
}


LANDING_TABLES_COLUMNS = {
    ('recorded_at', 'TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP'),
    ('api_name', 'STRING'),
    ('request', 'VARIANT'),
    ('error', 'VARIANT'),
    ('response', 'VARIANT'),
}


def make_call(
    service_account_info,
    service_name,
    service_version,
    resource_name,
    method,
    params,
    subject=None,
    scopes=None,
):
    creds = service_account.Credentials.from_service_account_info(service_account_info)
    if subject is not None:
        creds = creds.with_subject(subject).with_scopes(scopes)

    service = build(service_name, version=service_version, credentials=creds)
    resource_name, *subresource_names = resource_name.split('.')
    resource = getattr(service, resource_name)()
    for srn in subresource_names:
        resource = getattr(resource, srn)()

    return getattr(resource, method)(**params).execute()


def ingest(table_name, options, dryrun=False):
    service_user_creds = options['service_user_creds']
    for subject in options.get('subjects_list') or ['']:
        for api_name, spec in API_SPECS.items():
            landing_table = f'data.google_collect_{api_name}'
            call_params = {
                'subject': subject,
                'scopes': spec['scopes'],
                'service_name': spec['service_name'],
                'service_version': spec['service_version'],
                'resource_name': spec['resource_name'],
                'method': spec['method'],
                'params': spec['params'](subject=subject),
            }
            response = make_call(service_user_creds, **call_params)
            db.insert(
                f'data.{table_name}',
                {'api_name': api_name, 'response': response, 'request': call_params},
                dryrun=dryrun,
            )
            yield 1
