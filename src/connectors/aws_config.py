"""AWS Config
Collect Config logs from S3 using a privileged Role
"""
from json import dumps
from time import sleep

from runners.helpers import db
from runners.helpers.dbconfig import WAREHOUSE

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'bucket_name',
        'title': 'Config Bucket',
        'prompt': 'The S3 bucket Config puts your logs',
        'prefix': 's3://',
        'placeholder': 'my-test-s3-bucket',
        'required': True,
    },
    {
        'type': 'str',
        'name': 'filter',
        'title': 'Prefix Filter',
        'prompt': 'The folder in S3 bucket where Config puts logs',
        'default': 'AWSLogs/',
        'required': True,
    },
    {
        'type': 'str',
        'name': 'aws_role',
        'title': 'Config Bucket Reader Role',
        'prompt': "Role to be assumed for access to Config files in S3",
        'placeholder': 'arn:aws:iam::012345678987:role/my-config-reader-role',
        'required': True,
    },
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
    ('raw', 'VARIANT'),
    ('hash_raw', 'NUMBER'),
    ('event_time', 'TIMESTAMP_LTZ(9)'),
    ('configuration_item_capture_time', 'TIMESTAMP_LTZ(9)'),
    ('account_id', 'STRING'),
    ('aws_region', 'STRING'),
    ('arn', 'STRING'),
    ('availability_zone', 'STRING'),
    ('resource_creation_time', 'TIMESTAMP_LTZ(9)'),
    ('resource_name', 'STRING'),
    ('resource_Id', 'STRING'),
    ('resource_type', 'STRING'),
    ('relationships', 'VARIANT'),
    ('configuration', 'VARIANT'),
    ('tags', 'VARIANT'),
]


CONNECT_RESPONSE_MESSAGE = """
STEP 1: Modify the Role "{role}" to include the following trust relationship:

{role_trust_relationship}


STEP 2: For Role "{role}", add the following inline policy:

{role_policy}
"""


def connect(connection_name, options):
    base_name = f'AWS_CONFIG_{connection_name}_EVENTS'.upper()
    stage = f'data.{base_name}_STAGE'
    staging_table = f'data.{base_name}_STAGING'
    landing_table = f'data.{base_name}_CONNECTION'

    bucket = options['bucket_name']
    prefix = options.get('filter', 'AWSLogs/')
    role = options['aws_role']

    comment = f"""
---
module: aws_config
"""

    db.create_stage(
        name=stage,
        url=f's3://{bucket}',
        prefix=prefix,
        cloud='aws',
        credentials=role,
        file_format=FILE_FORMAT,
    )

    db.create_table(
        name=staging_table, cols=[('v', 'VARIANT'), ('filename', 'STRING(200)')]
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
    base_name = f'AWS_CONFIG_{connection_name}_EVENTS'.upper()
    pipe = f'data.{base_name}_PIPE'
    landing_table = f'data.{base_name}_CONNECTION'

    DATE_REGEXP = r'.+(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z.*'.replace(
        "\\", "\\\\"
    )
    DATE_ISO8601_BACKREFERENCES = r'\1-\2-\3T\4:\5:\6Z'.replace("\\", "\\\\")

    config_ingest_task = f'''
INSERT INTO {landing_table} (
  raw, hash_raw, event_time, configuration_item_capture_time, account_id, aws_region, resource_type, arn,
  availability_zone, resource_creation_time, resource_name, resource_Id, relationships, configuration, tags
)
SELECT value raw
    , HASH(value) hash_raw
    , REGEXP_REPLACE(filename, '{DATE_REGEXP}', '{DATE_ISO8601_BACKREFERENCES}')::TIMESTAMP_LTZ event_time
    , value:configurationItemCaptureTime::TIMESTAMP_LTZ(9) configuration_item_capture_time
    , value:awsAccountId::STRING account_id
    , value:awsRegion::STRING aws_region
    , value:resourceType::STRING aws_region
    , value:ARN::STRING arn
    , value:availabilityZone::STRING availability_zone
    , value:resourceCreationTime::TIMESTAMP_LTZ(9) resource_creation_time
    , value:resourceName::STRING resource_name
    , value:resourceId::STRING resource_Id
    , value:relationships::VARIANT relationships
    , value:configuration::VARIANT configuration
    , value:tags::VARIANT tags
FROM data.{base_name}_stream, LATERAL FLATTEN(input => v:configurationItems)
WHERE ARRAY_SIZE(v:configurationItems) > 0
'''

    db.create_stream(
        name=f'data.{base_name}_stream', target=f'data.{base_name}_staging'
    )

    # IAM change takes 5-15 seconds to take effect
    sleep(5)
    db.retry(
        lambda: db.create_pipe(
            name=pipe,
            sql=(
                f"COPY INTO data.{base_name}_staging(v, filename) "
                f"FROM (SELECT $1, metadata$filename FROM @data.{base_name}_stage/)"
            ),
            replace=True,
            autoingest=True,
        ),
        n=10,
        sleep_seconds_btw_retry=1,
    )

    db.create_task(
        name=f'data.{base_name}_TASK',
        schedule='1 minute',
        warehouse=WAREHOUSE,
        sql=config_ingest_task,
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
