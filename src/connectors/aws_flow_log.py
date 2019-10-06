"""AWS VPC Flow Logs
Collect VPC Flow logs from S3 using AssumeRole
"""

from time import sleep
from json import dumps

from runners.helpers import db

from .utils import yaml_dump

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'bucket_name',
        'title': "Flow Log Bucket",
        'prompt': "The S3 bucket where the Flow Logs are collected",
        'prefix': "s3://",
        'placeholder': "my-test-s3-bucket",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'filter',
        'title': "Prefix Filter",
        'prompt': "The folder in S3 bucket where Flow Logs are collected",
        'default': "AWSLogs/",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'aws_role',
        'title': "Flow Bucket Reader Role",
        'prompt': "Role to be assumed for access to Flow Logs in S3",
        'placeholder': "arn:aws:iam::012345678987:role/my-flow-log-reader-role",
        'required': True,
    },
]

FILE_FORMAT = db.TypeOptions(
    type='CSV', compression='GZIP', field_delimiter=' ', skip_header=1, null_if='-'
)

PROTOCOL_FILE_FORMAT = db.TypeOptions(
    type='CSV', compression='AUTO', skip_header=1, field_delimiter=',', null_if=''
)

LANDING_TABLE_COLUMNS = [
    ('VERSION', 'NUMBER'),
    ('ACCOUNT_ID', 'STRING(50)'),
    ('INTERFACE_ID', 'STRING(100)'),
    ('SRCADDR', 'STRING(50)'),
    ('DSTADDR', 'STRING(50)'),
    ('SRCPORT', 'NUMBER'),
    ('DSTPORT', 'NUMBER'),
    ('PROTOCOL', 'NUMBER'),
    ('PACKETS', 'NUMBER'),
    ('BYTES', 'NUMBER'),
    ('START_TIME', 'TIMESTAMP_LTZ'),
    ('END_TIME', 'TIMESTAMP_LTZ'),
    ('ACTION', 'STRING(250)'),
    ('LOG_STATUS', 'STRING(100)'),
]

PROTOCOL_MAPPING_TABLE_COLUMNS = [
    ('PROTOCOL_ID', 'NUMBER'),
    ('PROTOCOL_KEYWORD', 'STRING(50)'),
    ('PROTOCOL_NAME', 'STRING(100)'),
    ('PROTOCOL_IPV6_EXTENSION_HEADER', 'BOOLEAN'),
]

NETWORK_PROTOCOL_PATH = '../../connectors/protocol_table.csv'

CONNECT_RESPONSE_MESSAGE = """
STEP 1: Modify the Role "{role}" to include the following trust relationship:

{role_trust_relationship}


STEP 2: For Role "{role}", add the following inline policy:

{role_policy}
"""


def connect(connection_name, options):
    base_name = f'aws_vpc_flow_log_{connection_name}'
    stage = f'data.{base_name}_stage'
    landing_table = f'data.{base_name}_connection'

    bucket = options['bucket_name']
    prefix = options.get('filter', 'AWSLogs/')
    role = options['aws_role']

    comment = yaml_dump(module='aws_flow_log')

    db.create_stage(
        name=stage,
        url=f's3://{bucket}',
        prefix=prefix,
        cloud='aws',
        credentials=role,
        file_format=FILE_FORMAT,
    )

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment=comment)

    db.create_table_and_upload_csv(
        name='data.network_protocol_mapping',
        columns=PROTOCOL_MAPPING_TABLE_COLUMNS,
        file_path=NETWORK_PROTOCOL_PATH,
        file_format=PROTOCOL_FILE_FORMAT,
        ifnotexists=True,
    )

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
    base_name = f'aws_vpc_flow_log_{connection_name}'
    pipe = f'data.{base_name}_pipe'

    # IAM change takes 5-15 seconds to take effect
    sleep(5)
    db.retry(
        lambda: db.create_pipe(
            name=pipe,
            sql=(
                f'COPY INTO data.{base_name}_connection '
                f'FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14 '
                f'FROM @data.{base_name}_stage/)'
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
                f"If you'd like to backfill the table, please run\n\n  ALTER PIPE {pipe} REFRESH;"
            ),
        }
