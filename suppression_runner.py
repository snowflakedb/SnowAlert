import sys
import base64
import boto3
import json
import os
import snowflake.connector


def log_suppression(guid, alert_guid, ctx):
    #the alter set query
    query = "update snowalert.public.alerts set suppressed = TRUE, suppression_rule = '{0}' where alert:GUID = '{1}'".format(str(guid), str(alert_guid))
    ctx.cursor().execute(query)
    print("Suppression {} completed".format(guid))


def suppression_query(event):
    suppression_spec = event
    print("Received suppression {}".format(suppression_spec['GUID']))
    kms = boto3.client('kms')
    auth = kms.decrypt(CiphertextBlob = base64.b64decode(os.environ['auth']))['Plaintext'].decode()[:-1]
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
    try:
        results = ctx.cursor().execute(suppression_spec['Query']).fetchall()
    except Exception as e:
        print("Suppression query {} execution failed. Error: {}".format(suppression_spec['Query'], e))
        sys.exit(1)
    print("Suppression query {} executed".format(suppression_spec['GUID']))
    for res in results:
        alert_guid = json.loads(res[1])['GUID'] #  Make sure that you're checking the column where you select alert in the suppression query
        log_suppression(suppression_spec['GUID'], alert_guid, ctx)
        print("Event {} suppressed".format(alert_guid))


def lambda_handler(event, context):
    suppression_query(event)

