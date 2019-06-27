"""AWS Config
collects Config logs from S3 into a columnar table
"""

from json import dumps

from runners.helpers import db
from runners.helpers.dbconfig import WAREHOUSE
from time import sleep

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'bucket_name',
        'title': 'Config Bucket',
        'prompt': 'where Config puts your logs',
        'prefix': 's3://',
        'placeholder': 'my-test-s3-bucket',
        'required': True,
    },
    {
        'type': 'str',
        'name': 'filter',
        'title': 'Prefix Filter',
        'prompt': 'folder in S3 bucket where Config puts logs',
        'default': 'AWSLogs/',
        'required': True,
    },
    {
        'type': 'str',
        'name': 'aws_role',
        'title': 'AWS Role',
        'prompt': "ARN of Role we'll grant access to bucket",
        'placeholder': 'arn:aws:iam::012345678987:role/my-test-role',
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

CONFIG_LANDING_TABLE_COLUMNS = [
    ('v', 'VARIANT'),
    ('hash_raw', 'NUMBER'),
    ('event_time', 'TIMESTAMP_LTZ(9)'),
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
    ('tags', 'VARIANT')
]


CONNECT_RESPONSE_MESSAGE = """
STEP 1: Modify the Role "{role}" to include the following trust relationship:

{role_trust_relationship}


STEP 2: For Role "{role}", add the following inline policy:

{role_policy}
"""


def connect(connection_name, options):
    base_name = f'CONFIG_{connection_name}_EVENTS'.upper()
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
        file_format=FILE_FORMAT
    )

    db.create_table(
        name=staging_table,
        cols=[('v', 'variant')]
    )

    db.create_table(
        name=landing_table,
        cols=CONFIG_LANDING_TABLE_COLUMNS,
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
    base_name = f'CONFIG_{connection_name}_EVENTS'.upper()
    pipe = f'data.{base_name}_PIPE'
    landing_table = f'data.{base_name}_CONNECTION'

    # Step two: Configure the remainder once the role is properly configured.
    config_ingest_task = f"""
INSERT INTO {landing_table} (
  raw, hash_raw, config_capture_time, account_id, aws_region, resource_type, arn, availability_zone, 
  resource_creation_time, resource_name, resource_Id, relationships, configuration, tags
)
SELECT value raw
    , HASH(value) hash_raw
    , value:configurationItemCaptureTime::TIMESTAMP_LTZ(9) config_capture_time
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
FROM data.{base_name}_STREAM, lateral flatten(input => v:configurationItems)
WHERE ARRAY_SIZE(v:configurationItems) > 0
"""

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
                   warehouse=WAREHOUSE, sql=config_ingest_task)

    db.execute(f"ALTER PIPE {pipe} REFRESH")

    pipe_description = list(db.fetch(f'DESC PIPE {pipe}'))
    if len(pipe_description) < 1:
        return {
            'newStage': 'error',
            'newMessage': f"{pipe} does not exist; please reach out to Snowflake Security for assistance."
        }
    else:
        sqs_arn = pipe_description[0]['notification_channel']

    return {
        'newStage': 'finalized',
        'newMessage': (
            f"Please add this SQS Queue ARN to the bucket event notification "
            f"channel for all object create events: {sqs_arn}"
        )
    }


def test(base_name):
    yield db.fetch(f'ls @DATA.{base_name}_STAGE')
    yield db.fetch(f'DESC TABLE DATA.{base_name}_STAGING')
    yield db.fetch(f'DESC STREAM DATA.{base_name}_STREAM')
    yield db.fetch(f'DESC PIPE DATA.{base_name}_PIPE')
    yield db.fetch(f'DESC TABLE DATA.{base_name}_CONNECTION')
    yield db.fetch(f'DESC TASK DATA.{base_name}_TASK')
