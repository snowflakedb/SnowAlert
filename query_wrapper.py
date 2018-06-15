import sys
import base64
import boto3
import json
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
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


def login():
    kms = boto3.client('kms')

    password = kms.decrypt(CiphertextBlob=base64.b64decode(os.environ['private_key_password']))['Plaintext'].decode()
    private_key = serialization.load_pem_private_key(base64.b64decode(os.environ['private_key']), password=password.encode(), backend=default_backend())

    pkb = private_key.private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.TraditionalOpenSSL, encryption_algorithm=serialization.NoEncryption())

    try:
        ctx = snowflake.connector.connect(user='snowalert', account=os.environ['account'], private_key=pkb)
    except Exception as e:
        print("Failed to authenticate with error {}".format(e))
        sys.exit(1)
    return ctx


def lambda_handler(event, context):

    kms = boto3.client('kms')

    password = kms.decrypt(CiphertextBlob=base64.b64decode(os.environ['private_key_password']))['Plaintext'].decode()
    private_key = serialization.load_pem_private_key(base64.b64decode(os.environ['private_key']), password=password.encode(), backend=default_backend())

    pkb = private_key.private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.TraditionalOpenSSL, encryption_algorithm=serialization.NoEncryption())

    try:
        ctx = snowflake.connector.connect(user='snowalert', account=os.environ['account'], private_key=pkb)
    except Exception as e:
        print("Failed to authenticate with error {}".format(e))
        sys.exit(1)

    ctx.cursor().execute('select current_warehouse();')
    print(ctx.cursor().fetchall)
        
    load_queries(ctx)
