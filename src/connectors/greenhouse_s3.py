"""Greenhouse S3
Collect Greenhouse data from S3 using a privileged Role
"""

from json import dumps

from runners.helpers import db
from runners.helpers.dbconfig import WAREHOUSE
from time import sleep

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'bucket_name',
        'title': "Source S3 Bucket",
        'prompt': "Your S3 bucket where Greenhouse data is stored",
        'prefix': "s3://",
        'placeholder': "my-greenhouse-s3-bucket",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'aws_role',
        'title': "Greenhouse Bucket Reader Role",
        'prompt': "Role to be assumed for access to Greenhouse data in S3",
        'placeholder': "arn:aws:iam::012345678987:role/my-greenhouse-read-role",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'filter',
        'title': "Prefix Filter (optional)",
        'prompt': "Folder in S3 bucket where Greenhouse data is stored",
        'default': "",
        'required': True,
    },
]

FILE_FORMAT = db.TypeOptions(
    type='CSV',
    compression='GZIP',
    field_delimiter=' ',
    skip_header=1,
    null_if='-',
)

PROTOCOL_FILE_FORMAT = db.TypeOptions(
    type='CSV',
    compression='AUTO',
    skip_header=1,
    field_delimiter=',',
    null_if='',
)

# TODO(salma): Files are in csv format
LANDING_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
]


CONNECT_RESPONSE_MESSAGE = '''
STEP 1: Modify the Role "{role}" to include the following trust relationship:

{role_trust_relationship}


STEP 2: For Role "{role}", add the following inline policy:

{role_policy}
'''


def connect(connection_name, options):
    base_name = f'GREENHOUSE_S3_{connection_name}_EVENTS'.upper()
    stage = f'data.{base_name}_STAGE'
    staging_table = f'data.{base_name}_STAGING'
    landing_table = f'data.{base_name}_CONNECTION'

    bucket = options['bucket_name']
    prefix = options.get('filter', '')
    role = options['aws_role']

    comment = f'''
---
module: greenhouse_s3
'''

    db.create_stage(
        name=stage,
        url=f's3://{bucket}',
        prefix=prefix,
        cloud='aws',
        credentials=role,
        file_format=FILE_FORMAT
    )

    db.create_table(
        name=staging_table,
        cols=[('v', 'variant')]
    )

    db.create_table(
        name=landing_table,
        cols=LANDING_TABLE_COLUMNS,
        comment=comment
    )

    stage_props = db.fetch_props(
        f'DESC STAGE {stage}',
        filter=('AWS_EXTERNAL_ID', 'SNOWFLAKE_IAM_USER')
    )

    prefix = prefix.rstrip('/')

    return {
        'newStage': 'created',
        'newMessage': CONNECT_RESPONSE_MESSAGE.format(
            role=role,
            role_trust_relationship=dumps({
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Effect': 'Allow',
                        'Principal': {
                            'AWS': stage_props['SNOWFLAKE_IAM_USER']
                        },
                        'Action': 'sts:AssumeRole',
                        'Condition': {
                            'StringEquals': {
                                'sts:ExternalId': stage_props['AWS_EXTERNAL_ID']
                            }
                        }
                    }
                ]
            }, indent=4),
            role_policy=dumps({
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Effect': 'Allow',
                        'Action': [
                            's3:GetObject',
                            's3:GetObjectVersion',
                        ],
                        'Resource': f'arn:aws:s3:::{bucket}/{prefix}/*'
                    },
                    {
                        'Effect': 'Allow',
                        'Action': 's3:ListBucket',
                        'Resource': f'arn:aws:s3:::{bucket}',
                        'Condition': {
                            'StringLike': {
                                's3:prefix': [
                                    f'{prefix}/*'
                                ]
                            }
                        }
                    }
                ]
            }, indent=4),
        )
    }


def finalize(connection_name):
    base_name = f'GREENHOUSE_S3_{connection_name}_EVENTS'.upper()
    pipe = f'data.{base_name}_PIPE'
    landing_table = f'data.{base_name}_CONNECTION'

    # Step two: Configure the remainder once the role is properly configured.
    greenhouse_s3_ingest_task = f'''
INSERT INTO {landing_table} (
    raw
)
SELECT value raw
FROM data.{base_name}_STREAM, table(flatten(input => v:Records))
WHERE ARRAY_SIZE(v:Records) > 0
'''

    db.create_stream(
        name=f'data.{base_name}_STREAM',
        target=f'data.{base_name}_STAGING'
    )

    # IAM change takes 5-15 seconds to take effect
    sleep(5)
    db.retry(
        lambda: db.create_pipe(
            name=pipe,
            sql=f"COPY INTO data.{base_name}_STAGING(v) FROM @data.{base_name}_STAGE/",
            replace=True,
            autoingest=True
        ),
        n=10,
        sleep_seconds_btw_retry=1
    )

    db.create_task(name=f'data.{base_name}_TASK', schedule='1 minute',
                   warehouse=WAREHOUSE, sql=greenhouse_s3_ingest_task)

    pipe_description = next(db.fetch(f'DESC PIPE {pipe}'), None)
    if pipe_description is None:
        return {
            'newStage': 'error',
            'newMessage': f"{pipe} doesn't exist; please reach out to Snowflake Security for assistance."
        }
    else:
        sqs_arn = pipe_description['notification_channel']

    return {
        'newStage': 'finalized',
        'newMessage': (
            f"Please add this SQS Queue ARN to the bucket event notification"
            f"channel for all object create events:\n\n  {sqs_arn}\n\n"
            f"To backfill the landing table with existing data, please run:\n\n  ALTER PIPE {pipe} REFRESH;\n\n"
        )
    }
