"""GitHub Webhooks Connector from S3
Collect GitHub Webhooks from S3 bucket using a privileged Role
"""

from json import dumps
from time import sleep
from .utils import yaml_dump
from runners.helpers import db

S3_BUCKET_DEFAULT_PREFIX = ""

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'bucket_name',
        'title': "Webhook Bucket",
        'prompt': "The S3 bucket in which GitHub webhooks land",
        'prefix': "s3://",
        'placeholder': "my-test-s3-bucket",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'filter',
        'title': "Prefix Filter",
        'prompt': "The folder in Webhook Bucket containing webhook files",
        'default': S3_BUCKET_DEFAULT_PREFIX,
    },
    {
        'type': 'str',
        'name': 'aws_role',
        'title': "Reader Role",
        'prompt': "The AWS Role to be assumed for access to Webhook Bucket",
        'placeholder': "arn:aws:iam::012345678987:role/my-github-reader-role",
        'required': True,
    },
]

FILE_FORMAT = db.TypeOptions(
    type='JSON',
    compression='AUTO',
    enable_octal=False,
    allow_duplicate=False,
    strip_outer_array=True,
    strip_null_values=False,
    ignore_utf8_errors=False,
    skip_byte_order_mark=True,
)

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
    ('avatar_url', 'VARCHAR(256)'),
]

CONNECT_RESPONSE_MESSAGE = """
STEP 1: Modify the Role "{role}" to include the following trust relationship:
{role_trust_relationship}

STEP 2: For Role "{role}", add the following inline policy:
{role_policy}
"""


def connect(connection_name, options):
    base_name = f'GITHUB_WEBHOOKS_S3_{connection_name}_EVENTS'.upper()
    stage = f'data.{base_name}_STAGE'
    landing_table = f'data.{base_name}_CONNECTION'

    bucket = options['bucket_name']
    prefix = options.get('filter', S3_BUCKET_DEFAULT_PREFIX)
    role = options['aws_role']

    comment = yaml_dump(module='github_webhooks_s3')

    db.create_stage(
        name=stage,
        url=f's3://{bucket}',
        prefix=prefix,
        cloud='aws',
        credentials=role,
        file_format=FILE_FORMAT,
    )

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment=comment)

    stage_props = db.fetch_props(
        f'DESC STAGE {stage}', filter=('AWS_EXTERNAL_ID', 'SNOWFLAKE_IAM_USER')
    )

    prefix = prefix.rstrip('/')

    return {
        'newStage': 'created',
        'newMessage': CONNECT_RESPONSE_MESSAGE.format(
            role=role,
            role_trust_relationship=dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"AWS": stage_props['SNOWFLAKE_IAM_USER']},
                            "Action": "sts:AssumeRole",
                            "Condition": {
                                "StringEquals": {
                                    "sts:ExternalId": stage_props['AWS_EXTERNAL_ID']
                                }
                            },
                        }
                    ],
                },
                indent=4,
            ),
            role_policy=dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                            "Resource": f"arn:aws:s3:::{bucket}/{prefix}/*",
                        },
                        {
                            "Effect": "Allow",
                            "Action": "s3:ListBucket",
                            "Resource": f"arn:aws:s3:::{bucket}",
                            "Condition": {"StringLike": {"s3:prefix": [f"{prefix}/*"]}},
                        },
                    ],
                },
                indent=4,
            ),
        ),
    }


def finalize(connection_name):
    base_name = f'GITHUB_WEBHOOKS_S3_{connection_name}_EVENTS'.upper()
    pipe = f'data.{base_name}_PIPE'

    # IAM change takes 5-15 seconds to take effect
    sleep(5)
    db.retry(
        lambda: db.create_pipe(
            name=pipe,
            sql=(
                f"COPY INTO data.{base_name}_connection "
                f"FROM (SELECT current_timestamp, $1, HASH($1), $1:ref, $1: before, $1:after, $1:created, $1:deleted,"
                f"$1:forced, $1:base_ref, $1:compare, $1:commits, $1:head_commit,"
                f"$1:repository, $1:pusher, $1:organization, $1:sender, $1:action, $1:check_run, $1:check_suite, $1:number, $1:pull_request,"
                f"$1:label, $1:requested_team, $1:ref_type, $1:master_branch, $1:description, $1:pusher_type, $1:review, $1:changes, $1:comment, "
                f"$1:issue, $1:id, $1:sha, $1:name, $1:target_url, $1:context, $1:state, $1:commit, $1:branches, $1:created_at, $1:updated_at, $1:assignee, "
                f"$1:release, $1:membership, $1:alert, $1:scope, $1:member, $1:requested_reviewer, $1:team, $1:starred_at, $1:pages, $1:project_card, "
                f"$1:build, $1:deployment_status, $1:deployment, $1:forkee, $1:milestone, $1:key, $1:project_column, $1:status, $1:avatar_url FROM @data.{base_name}_stage/)"
            ),
            replace=True,
            autoingest=True,
        ),
        n=10,
        sleep_seconds_btw_retry=1,
    )

    pipe_description = next(db.fetch(f'DESC PIPE {pipe}'), None)
    if pipe_description is None:
        return {
            'newStage': 'error',
            'newMessage': f"{pipe} does not exist; please reach out to Snowflake Security for assistance.",
        }
    else:
        sqs_arn = pipe_description['notification_channel']

    return {
        'newStage': 'finalized',
        'newMessage': (
            f"Please add this SQS Queue ARN to the bucket event notification "
            f"channel for all object create events:\n\n  {sqs_arn}\n\n"
            f"To backfill the landing table with existing data, please run:\n\n  ALTER PIPE {pipe} REFRESH;\n\n"
        ),
    }
