"""osquery
Collect osquery from S3 using a Stage or privileged Role
"""
from json import dumps
from time import sleep

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE

from .utils import yaml_dump


CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'bucket_name',
        'title': "Source S3 Bucket",
        'prompt': "the S3 bucket where osquery logs go",
        'prefix': "s3://",
        'placeholder': "my-osquery-s3-bucket",
    },
    {
        'type': 'str',
        'name': 'aws_role',
        'title': "Bucket Reader Role",
        'prompt': "Role to be assumed for access to osquery files in S3",
        'placeholder': "arn:aws:iam::012345678987:role/my-osquery-read-role",
    },
    {
        'type': 'str',
        'name': 'prefix',
        'title': "Prefix Filter (optional)",
        'prompt': "Folder in S3 bucket where osquery logs are saved",
        'default': "security/",
    },
    {
        'type': 'str',
        'name': 'existing_stage',
        'title': "Snowflake Stage (alternative)",
        'prompt': "Enter to use an existing stage instead",
        'placeholder': "snowalert.data.osquery_stage",
    },
]


LANDING_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('hash_raw', 'NUMBER(19,0)'),
    ('event_time', 'TIMESTAMP_LTZ(9)'),
    ('action', 'VARCHAR(16777216)'),
    ('calendartime', 'VARCHAR(16777216)'),
    ('columns', 'VARIANT'),
    ('counter', 'NUMBER(38,0)'),
    ('epoch', 'NUMBER(38,0)'),
    ('hostidentifier', 'VARCHAR(16777216)'),
    ('instance_id', 'VARCHAR(16777216)'),
    ('name', 'VARCHAR(16777216)'),
    ('unixtime', 'TIMESTAMP_LTZ(9)'),
]


CONNECT_RESPONSE_MESSAGE = """
STEP 1: Be sure the Role "{role}" includes the following trust relationship:

{role_trust_relationship}


STEP 2: For Role "{role}", ensure the following inline policy:

{role_policy}
"""


def connect(connection_name, options):
    table_name = f'osquery_{connection_name}_connection'
    landing_table = f'data.{table_name}'

    db.create_table(
        name=landing_table,
        cols=LANDING_TABLE_COLUMNS,
        comment=yaml_dump(module='osquery')
    )
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')

    stage = options.get('existing_stage')
    if stage:
        stage_name = stage
    else:
        stage_name = f'data.osquery_{connection_name}_stage'
        bucket_name = options['bucket_name']
        prefix = options['prefix']
        aws_role = options['aws_role']
        db.create_stage(
            name=stage_name,
            url=f's3://{bucket_name}',
            prefix=prefix,
            cloud='aws',
            credentials=aws_role,
            file_format=db.TypeOptions(type='JSON')
        )

    stage_props = db.fetch_props(
        f'DESC STAGE {stage_name}',
        filter=('AWS_EXTERNAL_ID', 'SNOWFLAKE_IAM_USER', 'AWS_ROLE')
    )

    prefix = prefix.rstrip('/')

    return {
        'newStage': 'created',
        'newMessage': CONNECT_RESPONSE_MESSAGE.format(
            role=stage_props['AWS_ROLE'],  # this seems better than what we do in other places?
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
    base_name = f'osquery_{connection_name}'
    pipe = f'data.{base_name}_pipe'

    # IAM change takes 5-15 seconds to take effect
    sleep(5)
    db.retry(
        lambda: db.create_pipe(
            name=pipe,
            sql=(
                f'COPY INTO data.{base_name}_connection '
                f'FROM (SELECT PARSE_JSON($1), HASH($1), $1:unixTime::TIMESTAMP_LTZ(9), $1:action, $1:calendarTime, $1:columns, $1:counter, $1:epoch, $1:hostIdentifier, $1:instance_id, $1:name, $1:unixTime '
                f'FROM @data.{base_name}_stage/)'
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
