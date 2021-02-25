"""Google Collect
Collect Google API responses using a Service Account
"""

from google.oauth2 import service_account
from googleapiclient.discovery import build

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

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

API_SPECS = {
    'users': {
        'service_name': 'admin',
        'service_version': 'directory_v1',
        'scopes': ['https://www.googleapis.com/auth/admin.directory.user.readonly'],
        'resource_name': 'users',
        'method': 'list',
        'params': lambda subject: {'domain': subject.split('@')[1]},
    },
    'groups': {
        'service_name': 'admin',
        'service_version': 'directory_v1',
        'scopes': [
            "https://www.googleapis.com/auth/admin.directory.group.readonly",
        ],
        'resource_name': 'groups',
        'method': 'list',
        'params': lambda subject: {'domain': subject.split('@')[1]},
        'children': {
            'members': {
                'service_name': 'admin',
                'service_version': 'directory_v1',
                'scopes': [
                    "https://www.googleapis.com/auth/admin.directory.group.readonly",
                ],
                'resource_name': 'members',
                'method': 'list',
                'params': lambda parent: {'groupKey': parent['email']},
            }
        },
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


def ingest_helper(spec, api_name, lambda_arg, **kwargs):
    call_params = {
        'subject': kwargs['subject'],
        'scopes': spec['scopes'],
        'service_name': spec['service_name'],
        'service_version': spec['service_version'],
        'resource_name': spec['resource_name'],
        'method': spec['method'],
        'params': spec['params'](lambda_arg),
    }
    response = make_call(kwargs['service_user_creds'], **call_params)

    db.insert(
        f"data.{kwargs['table_name']}",
        {'api_name': api_name, 'response': response, 'request': call_params},
        dryrun=kwargs['dryrun'],
    )
    result = response.get(api_name, [])
    log.debug(f"Extracted {len(result)} {api_name}.")
    return result


def ingest(table_name, options, dryrun=False):
    # Contruct kwargs for helper function
    kwargs = {
        **options,
        'table_name': table_name,
        'dryrun': dryrun,
    }

    # Iterate over the list of subjects in domains
    # for which we want to list entities (users, groups, members)
    for subject in options.get('subjects_list') or ['']:
        for api_name, spec in API_SPECS.items():
            kwargs.update({'subject': subject})
            parents = ingest_helper(
                spec, api_name, subject, **kwargs
            )
            if 'children' not in spec:
                yield 1
            else:
                # This means we've come to a resource for which we're interested in the child resources
                # E.g. For groups() we would want to pull members()
                for parent in parents:
                    for child_api_name, child_spec in spec['children'].items():
                        ingest_helper(
                            child_spec, child_api_name, parent, **kwargs
                        )
                        yield 1
