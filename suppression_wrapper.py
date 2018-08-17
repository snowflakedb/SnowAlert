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

def flag_remaining_alerts(ctx):
        try:
            query = "update snowalert.public.alerts set suppressed = FALSE where suppressed is null;"
            ctx.cursor().execute(query)
        except Exception as e:
            print("Failed to flag remaining alerts as unsuppressed. Error: {}".format(e))

def execute_suppressions(suppression, lambda_client):
    try:
        print("Adding suppression {}".format(suppression['GUID']))
    except Exception as e:
        print("Suppression GUID missing. Error in suppression spec: {} with error {}".format(json.dumps(suppression), e))
    else:
        lambda_client.invoke(
            FunctionName = os.environ['SNOWALERT_SUPPRESSION_EXECUTOR_FUNCTION'],
            InvocationType = 'RequestResponse', #we need return values to make sure that the suppressions finish
            Payload = json.dumps(suppression).encode()
        )

def load_suppressions(ctx):
    try:
        suppression_spec_list = ctx.cursor().execute('select suppression_spec from snowalert.public.suppression_queries;').fetchall()
    except Exception as e:
        print("Loading suppression queries failed. Error: {}".format(e))
        sys.exit(1)
    print("Loaded {} suppressions successfully.".format(len(suppression_spec_list)))
    lambda_client = boto3.client('lambda')
    for suppression in suppression_spec_list:
        execute_suppressions(json.loads(suppression[0]), lambda_client)

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
        
    load_suppressions(ctx)
    flag_remaining_alerts(ctx)
