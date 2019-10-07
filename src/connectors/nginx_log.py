"""NGINX Log
Collect NGINX logs from S3 using a Stage or privileged Role
"""
from json import dumps
import re
from time import sleep
import yaml

from runners.helpers import db
from runners.helpers.dbconfig import ROLE as SA_ROLE

from .utils import yaml_dump


CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'bucket_name',
        'title': "Nginx Log Bucket",
        'prompt': "The S3 bucket where Nginx logs are ingested",
        'prefix': "s3://",
        'placeholder': "my-nginx-s3-bucket",
    },
    {
        'type': 'str',
        'name': 'aws_role',
        'title': "Bucket Reader Role",
        'prompt': "The Role to be assumed to access Nginx Log Bucket in S3",
        'placeholder': "arn:aws:iam::012345678987:role/my-nginx-reader",
    },
    {
        'type': 'str',
        'name': 'prefix',
        'title': "Log Prefix Filter",
        'prompt': "Location of nginx access and error folders in Nginx Log Bucket",
        'default': "operational_logs/",
    },
    {
        'type': 'str',
        'name': 'existing_stage',
        'title': "Snowflake Stage (alternative)",
        'prompt': "Enter to use an existing stage instead",
        'placeholder': "snowalert.data.nginx_log_existing_stage",
    },
]


LOG_LANDING_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('hash_raw', 'NUMBER(19,0)'),
    ('request_id', 'VARCHAR(16777216)'),
    ('event_time', 'TIMESTAMP_LTZ(9)'),
    ('bytes_sent', 'NUMBER(38,0)'),
    ('connection', 'NUMBER(38,0)'),
    ('connection_requests', 'NUMBER(38,0)'),
    ('deployment_cluster', 'VARCHAR(16777216)'),
    ('gzip_ratio', 'FLOAT'),
    ('host_header', 'VARCHAR(16777216)'),
    ('host_name', 'VARCHAR(16777216)'),
    ('user_agent', 'VARCHAR(16777216)'),
    ('http_user_agent', 'VARCHAR(16777216)'),
    ('http_xff', 'VARCHAR(16777216)'),
    ('http_referer', 'VARCHAR(16777216)'),
    ('http_method', 'VARCHAR(16777216)'),
    ('instance_id', 'VARCHAR(16777216)'),
    ('redirect_counter', 'NUMBER(38,0)'),
    ('remote_address', 'VARCHAR(16777216)'),
    ('request', 'VARCHAR(16777216)'),
    ('request_time', 'FLOAT'),
    ('requests_length', 'NUMBER(38,0)'),
    ('ssl_session_id', 'VARCHAR(16777216)'),
    ('ssl_session_reused', 'VARCHAR(16777216)'),
    ('status', 'NUMBER(38,0)'),
    ('event_time_other', 'VARIANT'),
    ('n_upstream_attempts', 'NUMBER(38,0)'),
    ('upstream_address', 'VARCHAR(16777216)'),
    ('upstream_response_length', 'NUMBER(38,0)'),
    ('upstream_response_time', 'FLOAT'),
    ('upstream_status', 'NUMBER(38,0)'),
]

ERROR_LANDING_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('hash_raw', 'NUMBER(19,0)'),
    ('instance_id', 'VARCHAR(16777216)'),
    ('log_level', 'VARCHAR(16777216)'),
    ('message', 'VARCHAR(16777216)'),
    ('event_time', 'TIMESTAMP_LTZ(9)'),
    ('pid', 'NUMBER(38,0)'),
    ('tid', 'NUMBER(38,0)'),
]

CONNECT_RESPONSE_MESSAGE = """
STEP 1: Be sure the Role "{role}" includes the following trust relationship:

{role_trust_relationship}


STEP 2: For Role "{role}", ensure the following inline policy:

{role_policy}
"""


def connect(connection_name, options):
    table_name = f'nginx_log_{connection_name}_connection'
    log_landing_table = f'data.{table_name}'
    error_landing_table = f'data.nginx_log_{connection_name}_error_connection'
    prefix = ''
    bucket_name = ''

    db.create_table(
        name=log_landing_table,
        cols=LOG_LANDING_TABLE_COLUMNS,
        comment=yaml_dump(module='nginx', **options),
    )
    db.execute(f'GRANT INSERT, SELECT ON {log_landing_table} TO ROLE {SA_ROLE}')

    db.create_table(
        name=error_landing_table,
        cols=ERROR_LANDING_TABLE_COLUMNS,
        comment=yaml_dump(module='nginx', **options),
    )
    db.execute(f'GRANT INSERT, SELECT ON {error_landing_table} TO ROLE {SA_ROLE}')

    stage_name = options.get('existing_stage')
    if not stage_name:
        stage_name = f'data.nginx_log_{connection_name}_stage'
        bucket_name = options['bucket_name']
        prefix = options['prefix']
        aws_role = options['aws_role']
        db.create_stage(
            name=stage_name,
            url=f's3://{bucket_name}',
            prefix=prefix,
            cloud='aws',
            credentials=aws_role,
            file_format=db.TypeOptions(type='JSON'),
        )

    stage_props = db.fetch_props(
        f'DESC STAGE {stage_name}',
        filter=('AWS_EXTERNAL_ID', 'SNOWFLAKE_IAM_USER', 'AWS_ROLE', 'URL'),
    )

    if not bucket_name or not prefix:
        m = re.match(r'^\["s3://([a-z-]*)/(.*)"\]$', stage_props['URL'])
        if m:
            bucket_name, prefix = m.groups()
        else:
            raise RuntimeError('cannot determine bucket name or prefix')

    prefix = prefix.rstrip('/')

    return {
        'newStage': 'created',
        'newMessage': CONNECT_RESPONSE_MESSAGE.format(
            role=stage_props[
                'AWS_ROLE'
            ],  # this seems better than what we do in other places?
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
                            "Resource": f"arn:aws:s3:::{bucket_name}/{prefix}/*",
                        },
                        {
                            "Effect": "Allow",
                            "Action": "s3:ListBucket",
                            "Resource": f"arn:aws:s3:::{bucket_name}",
                            "Condition": {"StringLike": {"s3:prefix": [f"{prefix}/*"]}},
                        },
                    ],
                },
                indent=4,
            ),
        ),
    }


def finalize(connection_name):
    base_name = f'nginx_log_{connection_name}'
    table = next(db.fetch(f"SHOW TABLES LIKE '{base_name}_connection' IN data"))
    options = yaml.load(table['comment'])
    stage = options.get('existing_stage', f'data.{base_name}_stage')
    pipe = f'data.{base_name}_pipe'
    error_pipe = f'data.{base_name}_error_pipe'

    # IAM change takes 5-15 seconds to take effect
    sleep(5)
    db.retry(
        lambda: db.create_pipe(
            name=pipe,
            sql=(
                f'COPY INTO data.{base_name}_connection '
                f"FROM (SELECT PARSE_JSON($1), HASH($1), regexp_substr($1:request::string, '.*request_?[iI]+d=([^\\\\&\\\\s]+)', 1, 1, 'e')"
                f", $1:time::timestamp_ltz, $1:bytes_sent::int, $1:connection::int, $1:connection_requests::int"
                f", nullif($1:deployment_cluster, '-')::string, nullif($1:gzip_ratio, '-')::float, $1:host_header::string"
                f", upper(split($1:\"host_header\"::string, '.')[0]), nullif(split(split($1:http_user_agent, '(' )[0], '/')[0], '-')::string , nullif($1:http_user_agent,'-')::string"
                f", nullif($1:http_xff, '-')::string, nullif($1:http_referer, '-')::string, regexp_substr($1:request, '^([A-Z]{{3,4}})\\\\s+.*', 1, 1, 'e')"
                f", $1:instance_id::string, $1:redirect_counter::int, $1:remote_address::string, $1:request::string, $1:request_time::float, $1:requests_length::int"
                f", nullif(strip_null_value($1:ssl_session_id),'-')::varchar, nullif(strip_null_value($1:ssl_session_reused),'-')::varchar, $1:status::int, $1:time"
                f", regexp_count($1:upstream_status, ' : ') + regexp_count($1:upstream_status, ', ') + 1::int "
                f", nullif(array_slice(split(array_slice(split($1:upstream_address, ' : '),-1,2)[0],', '),-1,2)[0],'-')::varchar "
                f", nullif(array_slice(split(array_slice(split($1:upstream_response_length, ' : '),-1,2)[0],', '),-1,2)[0],'-')::int "
                f", nullif(array_slice(split(array_slice(split($1:upstream_response_time, ' : '),-1,2)[0],', '),-1,2)[0],'-')::float "
                f", nullif(array_slice(split(array_slice(split($1:upstream_status, ' : '),-1,2)[0],', '),-1,2)[0],'-')::int "
                f'FROM @{stage}/access)'
            ),
            replace=True,
            autoingest=True,
        ),
        n=10,
        sleep_seconds_btw_retry=1,
    )

    db.retry(
        lambda: db.create_pipe(
            name=error_pipe,
            sql=(
                f"COPY INTO data.{base_name}_error_connection "
                f"FROM (SELECT PARSE_JSON($1), HASH($1), $1:instance_id::string, $1:log_level::string,"
                f" $1:message::string, $1:time::timestamp_ltz, $1:pid::int, $1:tid::int "
                f"FROM @{stage}/error)"
            ),
            replace=True,
            autoingest=True,
        ),
        n=10,
        sleep_seconds_btw_retry=1,
    )

    stage_props = db.fetch_props(f'DESC STAGE {stage}', filter=('URL'))
    stage_prefix = stage_props['URL'].split('/')[3]
    log_pipe_description = next(db.fetch(f'DESC PIPE {pipe}'), None)
    error_pipe_description = next(db.fetch(f'DESC PIPE {error_pipe}'), None)
    if log_pipe_description is None:
        return {
            'newStage': 'error',
            'newMessage': f"{pipe} does not exist; please reach out to Snowflake Security for assistance.",
        }
    elif error_pipe_description is None:
        return {
            'newStage': 'error',
            'newMessage': f"{error_pipe} does not exist; please reach out to Snowflake Security for assistance.",
        }

    else:
        log_sqs_arn = log_pipe_description['notification_channel']
        error_sqs_arn = error_pipe_description['notification_channel']
        return {
            'newStage': 'finalized',
            'newMessage': (
                f"Please add this SQS Queue ARN to the bucket event notification "
                f"channel for all object create events in the {stage_prefix}/access "
                f"folder of the bucket:\n\n  {log_sqs_arn}\n\n"
                f"Please add this SQS Queue ARN to the bucket event notification "
                f"channel for all object create events in the {stage_prefix}/error "
                f"folder of the bucket:\n\n   {error_sqs_arn}\n\n"
                f"Note that the two SQS Queue ARNs may be identical; this is normal.\n\n"
                f"If you'd like to backfill the table, please run\n\n"
                f"  ALTER PIPE {pipe} REFRESH;\n"
                f"  ALTER PIPE {error_pipe} REFRESH;"
            ),
        }
