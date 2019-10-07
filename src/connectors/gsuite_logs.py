"""G Suite Admin Logs
Collect G Suite API logs using a Service Account
"""

from googleapiclient.discovery import build
from google.oauth2 import service_account

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE

from .utils import yaml_dump

CONNECTION_OPTIONS = [
    {
        'name': 'connection_type',
        'type': 'select',
        'options': [
            # https://developers.google.com/admin-sdk/reports/v1/appendix/activity/login
            {'value': 'login', 'label': "Logins"}
        ],
        'title': "Admin Logs Type",
        'prompt': "The type of G Suite logs you are looking to collect.",
        'required': True,
        'default': 'login',
    },
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

LANDING_TABLES_COLUMNS = {
    'login': [
        ('created_on', 'TIMESTAMP_LTZ'),
        ('event_time', 'TIMESTAMP_LTZ'),
        ('etag', 'STRING(100)'),
        ('delegating_subject', 'STRING(500)'),
        ('event_name', 'STRING(50)'),
        ('event_params', 'VARIANT'),
        ('customer_id', 'STRING(100)'),
        ('actor_email', 'STRING(1000)'),
        ('actor_profile_id', 'STRING(1000)'),
        ('ip_address', 'STRING(100)'),
        ('raw', 'VARIANT'),
    ]
}

LOGIN_EVENTS = [
    'logout',
    'login_challenge',
    'login_failure',
    'login_verification',
    'login_success',
]
SCOPES = ['https://www.googleapis.com/auth/admin.reports.audit.readonly']


def connect(connection_name, options):
    connection_type = options['connection_type']
    base_name = f'gsuite_logs_{connection_name}_{connection_type}'
    landing_table = f'data.{base_name}_connection'
    comment = yaml_dump(module='gsuite_logs', **options)
    db.create_table(
        name=landing_table, cols=LANDING_TABLES_COLUMNS['login'], comment=comment
    )
    db.execute(f'GRANT INSERT, SELECT ON data.{base_name}_connection TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': 'Landing table created for collectors to populate.',
    }


def get_logs(service_account_info, with_subject=None, event_name='', start_time=None):
    creds = service_account.Credentials.from_service_account_info(service_account_info)
    if with_subject is not None:
        creds = creds.with_subject(with_subject).with_scopes(SCOPES)

    service = build('admin', version='reports_v1', credentials=creds)

    return (
        service.activities()
        .list(
            userKey='all',
            applicationName='login',
            eventName=event_name,
            startTime=start_time and start_time.isoformat(),
        )
        .execute()
    )


def ingest(table_name, options):
    landing_table = f'data.{table_name}'
    service_user_creds = options['service_user_creds']
    for subject in options.get('subjects_list') or ['']:
        for event in LOGIN_EVENTS:
            items = get_logs(
                service_user_creds,
                with_subject=subject,
                event_name=event,
                start_time=db.fetch_latest(
                    landing_table,
                    where=(
                        f"delegating_subject='{subject}' AND " f"event_name='{event}'"
                    ),
                ),
            ).get('items', [])

            db.insert(
                landing_table,
                values=[
                    (
                        item['id']['time'],
                        item['etag'].strip('"'),
                        subject,
                        item.get('events', [{}])[0].get('name'),
                        {
                            p['name']: (
                                p.get('value')
                                or p.get('boolValue')
                                or p.get('multiValue')
                            )
                            for p in item.get('events', [{}])[0].get('parameters', [])
                        },
                        item['id']['customerId'],
                        item['actor'].get('email'),
                        item['actor'].get('profileId'),
                        item.get('ipAddress'),
                        item,
                    )
                    for item in items
                ],
                select=(
                    'CURRENT_TIMESTAMP()',
                    'column1',
                    'column2',
                    'column3',
                    'column4',
                    'PARSE_JSON(column5)',
                    'column6',
                    'column7',
                    'column8',
                    'column9',
                    'PARSE_JSON(column10)',
                ),
            )
            yield len(items)
