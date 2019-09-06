"""LDAP Logs
Collect LDAP logs from S3 using AssumeRole
"""

from time import sleep
from json import dumps

from runners.helpers import db

from .utils import yaml_dump
import yaml

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'bucket_name',
        'title': "LDAP Log Bucket",
        'prompt': "The S3 bucket where the LDAP Logs are collected",
        'prefix': "s3://",
        'placeholder': "my-test-s3-bucket",
    },
    {
        'type': 'str',
        'name': 'prefix',
        'title': "Prefix Filter",
        'prompt': "The folder in S3 bucket where LDAP Logs are collected",
        'default': "ldap/",
    },
    {
        'type': 'str',
        'name': 'aws_role',
        'title': "Config Bucket Reader Role",
        'prompt': "Role to be assumed for access to LDAP Logs in S3",
        'placeholder': "arn:aws:iam::012345678987:role/my-flow-log-reader-role",
    },
    {
        'type': 'str',
        'name': 'existing_stage',
        'title': "Snowflake Stage (alternative)",
        'prompt': "Enter to use an existing stage instead",
        'placeholder': "snowalert.data.ldap_stage",
    },
]

FILE_FORMAT = db.TypeOptions(
    type='CSV',
    field_delimiter=',',
    skip_header=1,
    field_optionally_enclosed_by='"'
)

LANDING_TABLE_COLUMNS = [
    ('group_name', 'STRING(256)'),
    ('display_name', 'STRING(256)'),
    ('sam', 'STRING(100)'),
    ('email', 'STRING(256)'),
    ('account_created', 'TIMESTAMP_LTZ'),
    ('account_last_modified', 'TIMESTAMP_LTZ'),
    ('password_last_set', 'TIMESTAMP_LTZ'),
    ('password_expires', 'TIMESTAMP_LTZ'),
]

CONNECT_RESPONSE_MESSAGE = """
STEP 1: Modify the Role "{role}" to include the following trust relationship:

{role_trust_relationship}


STEP 2: For Role "{role}", add the following inline policy:

{role_policy}
"""


def connect(connection_name, options):
    base_name = f'ldap_{connection_name}'
    stage = f'data.{base_name}_stage'
    landing_table = f'data.{base_name}_connection'

    comment = yaml_dump(module='ldap', **options)

    stage = options.get('existing_stage')
    if stage:
        prefix = ''
        aws_role = ''
        bucket_name = ''
        stage_name = stage
    else:
        stage_name = f'data.ldap_{connection_name}_stage'
        bucket_name = options['bucket_name']
        prefix = options['prefix']
        aws_role = options['aws_role']
        db.create_stage(
            name=stage_name,
            url=f's3://{bucket_name}',
            prefix=prefix,
            cloud='aws',
            credentials=aws_role,
            file_format=FILE_FORMAT
        )

    db.create_table(
        name=landing_table,
        cols=LANDING_TABLE_COLUMNS,
        comment=comment
    )

    stage_props = db.fetch_props(
        f'DESC STAGE {stage_name}',
        filter=('AWS_EXTERNAL_ID', 'SNOWFLAKE_IAM_USER', 'AWS_ROLE', 'URL')
    )

    if prefix == '':
        prefix = '/'.join(stage_props['URL'].split('/')[3:-1])

    if bucket_name == '':
        bucket_name = stage_props['URL'].split('/')[2]

    if aws_role == '':
        aws_role = stage_props['AWS_ROLE']

    prefix = prefix.rstrip('/')

    return {
        'newStage': 'created',
        'newMessage': CONNECT_RESPONSE_MESSAGE.format(
            role=aws_role,
            role_trust_relationship=dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": stage_props['SNOWFLAKE_IAM_USER']
                        },
                        "Action": "sts:AssumeRole",
                        "Condition": {
                            "StringEquals": {
                                "sts:ExternalId": stage_props['AWS_EXTERNAL_ID']
                            }
                        }
                    }
                ]
            }, indent=4),
            role_policy=dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:GetObjectVersion",
                        ],
                        "Resource": f"arn:aws:s3:::{bucket_name}/{prefix}/*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": "s3:ListBucket",
                        "Resource": f"arn:aws:s3:::{bucket_name}",
                        "Condition": {
                            "StringLike": {
                                "s3:prefix": [
                                    f"{prefix}/*"
                                ]
                            }
                        }
                    }
                ]
            }, indent=4),
        )
    }


def finalize(connection_name):
    base_name = f'ldap_{connection_name}'
    pipe = f'data.{base_name}_pipe'
    options = yaml.load(next(db.fetch(f"SHOW TABLES LIKE '{base_name}_connection' IN data"))['comment'])
    stage = options.get('existing_stage', f'data.{base_name}_stage')

    # IAM change takes 5-15 seconds to take effect
    sleep(5)
    db.retry(
        lambda: db.create_pipe(
            name=pipe,
            sql=(
                f"COPY INTO data.{base_name}_connection "
                f"FROM (SELECT $1, $2, $3, $4, to_timestamp_ltz($5, 'mm/dd/yyyy hh24:mi:ss (UTC)'),"
                f" to_timestamp_ltz($6, 'mm/dd/yyyy hh24:mi:ss (UTC)'), to_timestamp_ltz($7, 'mm/dd/yyyy hh24:mi:ss (UTC)'),"
                f" to_timestamp_ltz($8, 'mm/dd/yyyy hh24:mi:ss (UTC)') "
                f"FROM @{stage}/)"
            ),
            replace=True,
            autoingest=True,
        ),
        n=10,
        sleep_seconds_btw_retry=1
    )

    pipe_description = next(db.fetch(f'DESC PIPE {pipe}'), None)
    if pipe_description is None:
        return {
            'newStage': 'error',
            'newMessage': f"{pipe} does not exist; please reach out to Snowflake Security for assistance."
        }

    else:
        sqs_arn = pipe_description['notification_channel']
        return {
            'newStage': 'finalized',
            'newMessage': (
                f"Please add this SQS Queue ARN to the bucket event notification "
                f"channel for all object create events:\n\n  {sqs_arn}\n\n"
                f"If you'd like to backfill the table, please run\n\n  ALTER PIPE {pipe} REFRESH;"
            )
        }
