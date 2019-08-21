"""GitHub Webhooks Connector from S3
Already collected Webhooks in S3 being placed in Snowflake
"""

from json import dumps
from time import sleep
from .utils import yaml_dump
from runners.helpers import db
from runners.helpers.dbconfig import WAREHOUSE

S3_BUCKET_DEFAULT_PREFIX = ""

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'bucket_name',
        'title': "GitHub Organization Bucket",
        'prompt': "The S3 bucket GitHub Organization puts your logs",
        'prefix': "s3://",
        'placeholder': "my-test-s3-bucket",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'filter',
        'title': "Prefix Filter",
        'prompt': "The folder in S3 bucket where GitHub Organization puts logs",
        'default': S3_BUCKET_DEFAULT_PREFIX,
        'required': True,
    },
    {
        'type': 'str',
        'name': 'aws_role',
        'title': "GitHub Organization Bucket Reader Role",
        'prompt': "Role to be assumed for access to GitHub Organization files in S3",
        'placeholder': "arn:aws:iam::012345678987:role/my-github-reader-role",
        'required': True,
    }
]

FILE_FORMAT = """
    TYPE = "JSON",
    COMPRESSION = "AUTO",
    ENABLE_OCTAL = FALSE,
    ALLOW_DUPLICATE = FALSE,
    STRIP_OUTER_ARRAY = TRUE,
    STRIP_NULL_VALUES = FALSE,
    IGNORE_UTF8_ERRORS = FALSE,
    SKIP_BYTE_ORDER_MARK = TRUE
"""

LANDING_TABLE_COLUMNS = [
    ('insert_time', 'TIMESTAMP_LTZ(9)'),
    ('raw', 'VARIANT'),
    ('hash_raw', 'NUMBER'),
    ('ref', 'VARCHAR(256)'),
    ('before', 'VARCHAR(256)'),
    ('after', 'VARCHAR(256)'),
    ('created', 'BOOLEAN'),
    ('deleted', 'BOOLEAN'),
    ('forced', 'BOOLEAN'),
    ('base_ref', 'VARCHAR(256)'),
    ('compare', 'VARCHAR(256)'),
    ('commits', 'VARIANT'),
    ('head_commit', 'VARIANT'),
    ('repository', 'VARIANT'),
    ('pusher', 'VARIANT'),
    ('organization', 'VARIANT'),
    ('sender', 'VARIANT'),
    ('action', 'VARCHAR(256)'),
    ('check_run', 'VARIANT'),
    ('check_suite', 'VARIANT'),
    ('number', 'NUMBER(38,0)'),
    ('pull_request', 'VARIANT'),
    ('label', 'VARIANT'),
    ('requested_team', 'VARIANT'),
    ('ref_type', 'VARCHAR(256)'),
    ('master_branch', 'VARCHAR(256)'),
    ('description', 'VARCHAR(256)'),
    ('pusher_type', 'VARCHAR(256)'),
    ('review', 'VARIANT'),
    ('changes', 'VARIANT'),
    ('comment', 'VARIANT'),
    ('issue', 'VARIANT'),
    ('id', 'NUMBER(38,0)'),
    ('sha', 'VARCHAR(256)'),
    ('name', 'VARCHAR(256)'),
    ('target_url', 'VARCHAR(8192)'),
    ('context', 'VARCHAR(256)'),
    ('state', 'VARCHAR(256)'),
    ('commit', 'VARIANT'),
    ('branches', 'VARIANT'),
    ('created_at', 'TIMESTAMP_LTZ(9)'),
    ('updated_at', 'TIMESTAMP_LTZ(9)'),
    ('assignee', 'VARIANT'),
    ('release', 'VARIANT'),
    ('membership', 'VARIANT'),
    ('alert', 'VARIANT'),
    ('scope', 'VARCHAR(256)'),
    ('member', 'VARIANT'),
    ('requested_reviewer', 'VARIANT'),
    ('team', 'VARIANT'),
    ('starred_at', 'TIMESTAMP_LTZ(9)'),
    ('pages', 'VARIANT'),
    ('project_card', 'VARIANT'),
    ('build', 'VARIANT'),
    ('deployment_status', 'VARIANT'),
    ('deployment', 'VARIANT'),
    ('forkee', 'VARIANT'),
    ('milestone', 'VARIANT'),
    ('key', 'VARIANT'),
    ('project_column', 'VARIANT'),
    ('status', 'VARCHAR(256)'),
    ('avatar_url', 'VARCHAR(256)')
]

CONNECT_RESPONSE_MESSAGE = """
STEP 1: Modify the Role "{role}" to include the following trust relationship:
{role_trust_relationship}
STEP 2: For Role "{role}", add the following inline policy:
{role_policy}
"""


def connect(connection_name, options):
    base_name = f'GITHUB_ORGANIZATION_{connection_name}_EVENTS'.upper()
    stage = f'data.{base_name}_STAGE'
    landing_table = f'data.{base_name}_CONNECTION'

    bucket = options['bucket_name']
    prefix = options.get('filter', S3_BUCKET_DEFAULT_PREFIX)
    role = options['aws_role']

    comment = yaml_dump(
        module='github_organization',
    )

    db.create_stage(
        name=stage,
        url=f's3://{bucket}',
        prefix=prefix,
        cloud='aws',
        credentials=role,
        file_format=FILE_FORMAT
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
                        "Resource": f"arn:aws:s3:::{bucket}/{prefix}/*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": "s3:ListBucket",
                        "Resource": f"arn:aws:s3:::{bucket}",
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
    base_name = f'GITHUB_ORGANIZATION_{connection_name}_EVENTS'.upper()
    pipe = f'data.{base_name}_PIPE'

    # IAM change takes 5-15 seconds to take effect
    sleep(5)
    db.retry(
        lambda: db.create_pipe(
            name=pipe,
            sql=(
                f"COPY INTO data.{base_name}_connection "
                f"FROM (SELECT $1 FROM @data.{base_name}_stage/)"
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
            f"To backfill the landing table with existing data, please run:\n\n  ALTER PIPE {pipe} REFRESH;\n\n"
        )
    }
