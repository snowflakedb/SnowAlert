"""GCP Cloud Asset
Collect GCP Cloud Assets via API and PIPE
"""
from datetime import datetime

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
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
        'name': 'org_locations',
        'title': "Organization Locations",
        'prompt': "Comma separated lists of org_id:gcs_bucket",
        'placeholder': "130280171936:snowalert-cloudops-inventory,...",
    },
]

LANDING_TABLES_COLUMNS = {
    'export': [
        ('created_on', 'TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP'),
        ('org_id', 'STRING'),
        ('raw', 'VARIANT'),
    ]
}


def start_export_assets_job(client, org_id, gcs_uri_prefix, content_type):
    try:
        result = (
            client.v1()
            .exportAssets(
                parent=f'organizations/{org_id}',
                body={
                    'outputConfig': {'gcsDestination': {'uriPrefix': gcs_uri_prefix}},
                    'assetTypes': ['.*'],
                    'contentType': content_type,
                },
            )
            .execute()
        )
    except HttpError as e:
        result = {
            'org_id': org_id,
            'gcs_uri_prefix': gcs_uri_prefix,
            'error': str(e),
            'error_details': e.error_details,
        }

    return {'raw': result, 'org_id': org_id}


def ingest(table_name, options):
    landing_table = f'data.{table_name}'

    creds = service_account.Credentials.from_service_account_info(
        options['service_user_creds']
    )
    client = build('cloudasset', version='v1', credentials=creds)

    # https://cloud.google.com/asset-inventory/docs/reference/rpc/google.cloud.asset.v1#google.cloud.asset.v1.ContentType
    content_types = ['RESOURCE', 'IAM_POLICY', 'ORG_POLICY', 'ACCESS_POLICY']

    for org_location in options['org_locations'].split(','):
        org_id, location = org_location.split(':')
        dt = datetime.utcnow().strftime('%Y/%m/%d/%H:%M:%S')
        for content_type in content_types:
            prefix = 'gs://' + location + '/cloudassets/' + content_type + '/' + dt      
            db.insert(landing_table, start_export_assets_job(client, org_id, prefix, content_type)) 