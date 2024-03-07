from collections import namedtuple
from datetime import datetime

from botocore.exceptions import BotoCoreError

from connectors.aws_collect import process_aws_response, DBEntry, CollectTask


Sample = namedtuple('Sample', ['task', 'response', 'entities', 'subrequests'])


class AnyDate(object):
    def __eq__(self, other):
        return isinstance(other, datetime)


TEST_DATA_REQUEST_RESPONSE = [
    # e.g. error
    Sample(
        CollectTask('1', 'iam.list_account_aliases', {}),
        BotoCoreError(),
        [
            DBEntry(
                {
                    'recorded_at': AnyDate(),
                    'account_id': '1',
                    'error': {
                        'message': 'botocore.exceptions.BotoCoreError: An unspecified error occurred',
                        'exceptionName': 'BotoCoreError',
                        'exceptionArgs': ('An unspecified error occurred',),
                        'exceptionTraceback': 'botocore.exceptions.BotoCoreError: An unspecified error occurred\n',
                        'responseMetadata': {},
                    },
                }
            )
        ],  # BotoErrors should record time
        [],
    ),
    # e.g. list-of-entities response
    Sample(
        CollectTask('1', 'kms.list_keys', {}),
        {
            "Keys": [
                {'KeyId': 'id1', 'KeyArn': 'arn1'},
                {'KeyId': 'id2', 'KeyArn': 'arn2'},
            ],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {'date': '2020-01-01T00:00:00'},
            },
        },
        [
            DBEntry(
                {
                    'recorded_at': AnyDate(),
                    'account_id': '1',
                    "key_id": "id1",
                    "key_arn": "arn1",
                }
            ),
            DBEntry(
                {
                    'recorded_at': AnyDate(),
                    'account_id': '1',
                    "key_id": "id2",
                    "key_arn": "arn2",
                }
            ),
        ],
        [
            CollectTask('1', 'kms.get_key_rotation_status', {'KeyId': 'arn1'}),
            CollectTask('1', 'kms.get_key_rotation_status', {'KeyId': 'arn2'}),
        ],
    ),
    # e.g. list-of-strings response
    Sample(
        CollectTask('1', 'iam.list_account_aliases', {}),
        {
            'AccountAliases': ['one', 'two'],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {'date': '2020-01-01T00:00:00'},
            },
        },
        [
            DBEntry(
                {'recorded_at': AnyDate(), 'account_id': '1', 'account_alias': 'one'}
            ),
            DBEntry(
                {'recorded_at': AnyDate(), 'account_id': '1', 'account_alias': 'two'}
            ),
        ],
        [],
    ),
    #  e.g. single-entity response with custom parser
    Sample(
        CollectTask('1', 'iam.get_credential_report', {}),
        {
            'Content': 'col1,col2\nval11,val12\nval21,val22',
            'ReportFormat': 'csv',
            'GeneratedTime': '2019-11-30T12:13:14Z',
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {'date': '2020-01-01T00:00:00'},
            },
        },
        [
            DBEntry(
                {
                    'recorded_at': AnyDate(),
                    'account_id': '1',
                    'generated_time': '2019-11-30T12:13:14Z',
                    'report_format': 'csv',
                    'content': 'col1,col2\nval11,val12\nval21,val22',
                    'content_csv_parsed': [
                        {'col1': 'val11', 'col2': 'val12'},
                        {'col1': 'val21', 'col2': 'val22'},
                    ],
                }
            )
        ],
        [],
    ),
    # e.g. repeat-field list-of-entities response
    Sample(
        CollectTask('1', 's3.list_buckets', {}),
        {
            'Owner': {'DisplayName': 'dn1', 'ID': 'oid1'},
            'Buckets': [
                {'Name': 'name1', 'CreationDate': 'date1'},
                {'Name': 'name2', 'CreationDate': 'date2'},
            ],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {'date': '2020-01-01T00:00:00'},
            },
        },
        [
            DBEntry(
                {
                    'recorded_at': AnyDate(),
                    'account_id': '1',
                    'owner_display_name': 'dn1',
                    'owner_id': 'oid1',
                    'bucket_name': 'name1',
                    'bucket_creation_date': 'date1',
                }
            ),
            DBEntry(
                {
                    'recorded_at': AnyDate(),
                    'account_id': '1',
                    'owner_display_name': 'dn1',
                    'owner_id': 'oid1',
                    'bucket_name': 'name2',
                    'bucket_creation_date': 'date2',
                }
            ),
        ],
        [
            CollectTask(
                account_id='1', method='s3.get_bucket_acl', args={'Bucket': 'name1'}
            ),
            CollectTask(
                account_id='1', method='s3.get_bucket_policy', args={'Bucket': 'name1'}
            ),
            CollectTask(
                account_id='1', method='s3.get_bucket_logging', args={'Bucket': 'name1'}
            ),
            CollectTask(
                account_id='1', method='s3.get_bucket_tagging', args={'Bucket': 'name1'}
            ),
            CollectTask(
                account_id='1',
                method='s3.get_public_access_block',
                args={'Bucket': 'name1'},
            ),
            CollectTask(
                account_id='1', method='s3.get_bucket_acl', args={'Bucket': 'name2'}
            ),
            CollectTask(
                account_id='1', method='s3.get_bucket_policy', args={'Bucket': 'name2'}
            ),
            CollectTask(
                account_id='1', method='s3.get_bucket_logging', args={'Bucket': 'name2'}
            ),
            CollectTask(
                account_id='1', method='s3.get_bucket_tagging', args={'Bucket': 'name2'}
            ),
            CollectTask(
                account_id='1',
                method='s3.get_public_access_block',
                args={'Bucket': 'name2'},
            ),
        ],
    ),
    # e.g. with parameter
    Sample(
        CollectTask('1', 'kms.get_key_rotation_status', {'KeyId': 'arn1'}),
        {
            'KeyRotationEnabled': True,
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {'date': '2020-01-01T00:00:00'},
            },
        },
        [
            DBEntry(
                {
                    'recorded_at': AnyDate(),
                    'account_id': '1',
                    'key_arn': 'arn1',
                    'key_rotation_enabled': True,
                }
            )
        ],
        [],
    ),
]


def test_process_aws_response():
    for sample in TEST_DATA_REQUEST_RESPONSE:
        db_entries = []
        child_requests = []
        for r in process_aws_response(sample.task, sample.response):
            if type(r) is DBEntry:
                db_entries.append(r)
            elif type(r) is CollectTask:
                child_requests.append(r)
        assert sample.entities == db_entries
        assert sample.subrequests == child_requests
