"""Google Collect
Collect Google API responses using a Service Account
"""

from google.oauth2 import service_account
from googleapiclient.discovery import build

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE

from .utils import apply_part

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
        'name': 'collect',
        'title': "List of Collections",
        'prompt': "Comma-separated list of domain, subject, orgId",
        'placeholder': '{"domain":"...","subject":"...","orgId":"..."}',
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
    'findings': {
        'service_name': 'securitycenter',
        'service_version': 'v1',
        'scopes': ['https://www.googleapis.com/auth/cloud-platform'],
        'resource_name': 'organizations.sources.findings',
        'method': 'list',
        'without_subject': True,
        'params': lambda orgId: {
            'parent': f'organizations/{orgId}/sources/-'
        },
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
    without_subject=False,
    subject=None,
    scopes=None,
):
    c = service_account.Credentials.from_service_account_info(service_account_info)
    if subject is None or without_subject:
        creds = c.with_scopes(scopes)
    else:
        creds = c.with_subject(subject).with_scopes(scopes)


    service = build(service_name, version=service_version, credentials=creds)
    resource_name, *subresource_names = resource_name.split('.')
    resource = getattr(service, resource_name)()
    for srn in subresource_names:
        resource = getattr(resource, srn)()

    return getattr(resource, method)(**params).execute()


def ingest_helper(spec, api_name, service_user_creds, table_name, dryrun, **kwargs):
    call_params = {**kwargs, **spec}
    call_params['params'] = apply_part(spec['params'], **call_params)
    response = apply_part(make_call, service_user_creds, **call_params)

    db.insert(
        f"data.{table_name}",
        {'api_name': api_name, 'response': response, 'request': call_params},
        dryrun=dryrun,
    )
    result = response.get(api_name, [])
    log.debug(f"Extracted and Loaded {len(result)} {api_name}.")
    return result


def ingest(table_name, options, dryrun=False):
    # Construct kwargs for helper function
    kwargs = {
        'table_name': table_name,
        'dryrun': dryrun,
        **options,
    }

    # Iterate over the list of domains, subjects, orgId's
    # for which we want to list entities (users, groups, members)
    for collectee in options.get('collect') or ['']:
        apis = options['apis'].split(',') if 'apis' in options else API_SPECS
        for api_name in apis:
            spec = API_SPECS[api_name]
            kwargs.update(collectee)
            parents = ingest_helper(spec, api_name, **kwargs)
            if 'children' not in spec:
                yield 1
            else:
                # This means we've come to a resource for which we're interested in the child resources
                # E.g. For groups() we would want to pull members()
                for parent in parents:
                    for child_api_name, child_spec in spec['children'].items():
                        kwargs['parent'] = parent
                        ingest_helper(child_spec, child_api_name, **kwargs)
                        yield 1
