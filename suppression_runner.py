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

def do_suppression(suppression_guid, suppression_query, ctx):
    # set alert instances matching the suppression to suppressed
    query = """merge into snowalert.public.alerts t
    using(
    {0}) s
    on t.alert:GUID = s.alert:GUID  
    when matched then update
    set t.SUPPRESSED = 'true', t.SUPPRESSION_RULE = '{1}';
    """.format(str(suppression_query), str(suppression_guid))
    ctx.cursor().execute(query)


def run_suppressions(event):
    suppression_spec = event
    print("Received suppression {}".format(suppression_spec['GUID']))

    kms = boto3.client('kms')
    password = kms.decrypt(CiphertextBlob=base64.b64decode(os.environ['private_key_password']))['Plaintext'].decode()

    private_key = serialization.load_pem_private_key(base64.b64decode(os.environ['private_key']), password=password.encode(), backend=default_backend())

    pkb = private_key.private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.TraditionalOpenSSL, encryption_algorithm=serialization.NoEncryption())

    try:
        ctx = snowflake.connector.connect(user='snowalert', account=os.environ['account'], private_key=pkb)
    except Exception as e:
        print("Failed to authenticate with error {}".format(e))
        sys.exit(1)

    try:
        do_suppression(suppression_spec['GUID'], suppression_spec['Query'], ctx)
    except Exception as e:
        print("Suppression query {} execution failed. Error: {}".format(suppression_spec['Query'], e))
        sys.exit(1)
    print("Suppression query {} executed".format(suppression_spec['GUID']))


def lambda_handler(event, context):
    run_suppressions(event)
