import sys
import base64
import boto3
import json
import os
import snowflake.connector


def load_queries(ctx):
    try:
        query_spec_list = ctx.cursor().execute('select query_spec from snowalert.public.snowalert_queries;').fetchall()
    except Exception as e:
        print("Loading SnowAlert queries failed. Error: {}".format(e))
        sys.exit(1)
    print("Loaded {} queries successfully.".format(len(query_spec_list)))
    lambda_client = boto3.client('lambda')
    for query_spec in query_spec_list:
        execute_queries(json.loads(query_spec[0]), lambda_client)


def execute_queries(query_spec, lambda_client):
    try:
        print("Adding query {}".format(query_spec['GUID']))
    except Exception as e:
        print("Query GUID missing. Error in query spec: {}".format(json.dumps(query_spec)))
    else:
        lambda_client.invoke(
            FunctionName=os.environ['SNOWALERT_QUERY_EXECUTOR_FUNCTION'],
            InvocationType='Event',
            Payload=json.dumps(query_spec).encode()
        )


def lambda_handler(event, context):
    kms = boto3.client('kms')
    auth = kms.decrypt(CiphertextBlob=base64.b64decode(os.environ['auth']))['Plaintext'].decode()[:-1]
    try:
        ctx = snowflake.connector.connect(
            user='snowalert',
            account=os.environ['SNOWALERT_ACCOUNT'],
            password=auth,
            warehouse='snowalert'
        )
    except Exception as e:
        print("Snowflake connection failed. Error: {}".format(e))
        sys.exit(1)
    load_queries(ctx)
