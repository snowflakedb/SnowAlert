"""GitHub Webhooks to S3
Collecting Webhooks from GitHub to S3 using an API Gateway, a Lambda, and Firehose.
"""

from json import dumps
from time import sleep

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE

from .utils import yaml_dump
import requests

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
    landing_table = f'data.{base_name}_CONNECTION'

    comment = yaml_dump(
        module='github_organization',
    )

    db.create_table(
        name=landing_table,
        cols=LANDING_TABLE_COLUMNS,
        comment=comment
    )

    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')
    return {
        'newStage': 'finalized',
        'newMessage': "GitHub Organization ingestion table created!",
    }


def finalize(connection_name):
    base_name = f'GITHUB_ORGANIZATION_{connection_name}_EVENTS'.upper()
    pipe = f'data.{base_name}_PIPE'
    landing_table = f'data.{base_name}_CONNECTION'

    lambda: db.create_pipe(
        name=pipe,
        sql=(
            f'''
                SELECT CURRENT_TIMESTAMP() insert_time
                    , value raw
                    , HASH(value) hash_raw
                    , value:ref::VARCHAR(256) ref
                    , value:before::VARCHAR(256) before
                    , value:after::VARCHAR(256) after
                    , value:created::BOOLEAN created
                    , value:deleted::BOOLEAN deleted
                    , value:forced::BOOLEAN forced
                    , value:base_ref::VARCHAR(256) base_ref
                    , value:compare::VARCHAR(256) compare
                    , value:commits::VARIANT commits
                    , value:head_commit::VARIANT head_commit
                    , value:repository::VARIANT repository
                    , value:pusher::VARIANT pusher
                    , value:organization::VARIANT organization
                    , value:sender::VARIANT sender
                    , value:action::VARCHAR(256) action
                    , value:check_run::VARIANT check_run
                    , value:check_suite::VARIANT check_suite
                    , value:number::NUMBER(38,0) number
                    , value:pull_request::VARIANT pull_request
                    , value:label::VARIANT label
                    , value:requested_team::VARIANT requested_team
                    , value:ref_type::VARCHAR(256) ref_type
                    , value:master_branch::VARCHAR(256) master_branch
                    , value:description::VARCHAR(256) description
                    , value:pusher_type::VARCHAR(256) pusher_type
                    , value:review::VARIANT review
                    , value:changes::VARIANT changes
                    , value:comment::VARIANT comment
                    , value:issue::VARIANT issue
                    , value:id::NUMBER(38,0) id
                    , value:sha::VARCHAR(256) sha
                    , value:name::VARCHAR(256) name
                    , value:target_url::VARCHAR(8192) target_url
                    , value:context:: VARCHAR(256) context
                    , value:state:: VARCHAR(256) state
                    , value:commit::VARIANT commit
                    , value:branches:VARIANT branches
                    , value:created_at::TIMESTAMP_LTZ(9) created_at
                    , value:updated_at::TIMESTAMP_LTZ(9) updated_at
                    , value:assignee::VARIANT assignee
                    , value:release::VARIANT release
                    , value:membership::VARIANT membership
                    , value:alert::VARIANT alert
                    , value:scope::VARCHAR(256) scope
                    , value:member:VARIANT member
                    , value:requested_reviewer::VARIANT requested_reviewer
                    , value:team::VARIANT team
                    , value:starred_at::TIMESTAMP_LTZ(9) starred_at
                    , value:pages::VARIANT pages
                    , value:project_card::VARIANT project_card
                    , value:build::VARIANT build
                    , value:deployment_status::VARIANT deployment_status
                    , value:deployment::VARIANT deployment
                    , value:forkee::VARIANT forkee
                    , value:milestone::VARIANT milestone
                    , value:key::VARIANT key
                    , value:project_column::VARIANT project_column
                    , value:status::VARCHAR(256) status
                    , value:avatar_url::VARCHAR(256) avatar_url
                FROM data.{base_name}_stream
                '''
        )
    )

    db.insert(
        landing_table,
        values=[(row, row['published']) for row in result],
        select='PARSE_JSON(column1), column2'
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
