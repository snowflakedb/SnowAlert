import os
import json
import snowflake.connector
import plugins
import base64
import sys
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
import boto3


def db_connect():
    if os.environ.get('PROD_FLAG'):
        kms = boto3.client('kms')
        password = kms.decrypt(CiphertextBlob=base64.b64decode(os.environ['private_key_password']))['Plaintext'].decode()

        private_key = serialization.load_pem_private_key(base64.b64decode(os.environ['private_key']), password=password.encode(), backend=default_backend())

        pkb = private_key.private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.TraditionalOpenSSL, encryption_algorithm=serialization.NoEncryption())

        try:
            connection = snowflake.connector.connect(user='snowalert', account=os.environ['SNOWALERT_ACCOUNT'], private_key=pkb)
        except Exception as e:
            print("Failed to authenticate with error {}".format(e))
            sys.exit(1)
    else:
        auth = os.environ['SNOWALERT_PASSWORD']

        connection = snowflake.connector.connect(
            account=os.environ['SNOWALERT_ACCOUNT'],
            user=os.environ['SNOWALERT_USER'],
            password=str(auth)
        )
    connection.cursor().execute('use warehouse snowalert')
    connection.cursor().execute('use database snowalert')
    return connection


def get_new_alerts(connection):
    get_alerts_query = 'select * from snowalert.public.alerts where ticket is null and suppressed = FALSE'
    results = connection.cursor().execute(get_alerts_query).fetchall()
    print('Found', len(results), 'new alerts.')
    print(results)
    return results


def record_ticket_id(connection, ticket_id, guid):
    query = "update snowalert.public.alerts set ticket = '" + str(ticket_id) + "' where alert:GUID = '" + str(guid) + "'"
    print('Updating alert table:', query)
    connection.cursor().execute(query)


def lambda_handler(event, context):
    ctx = db_connect()
    alerts = get_new_alerts(ctx)
    print('Found', len(alerts), 'new alerts to handle.')

    for row in alerts:
        alert = json.loads(row[0])
        print('Creating ticket for alert', alert)

        # Create a new ticket in JIRA for the alert
        ticket_id = plugins.create_jira_ticket(
            guid=alert['GUID'],
            alertTime=alert['AlertTime'],
            severity=alert['Severity'],
            detector=alert['Detector'],
            env=alert['AffectedEnv'],
            objectType=alert['AffectedObjectType'],
            object=alert['AffectedObject'],
            alertType=alert['AlertType'],
            description=alert['Description'],
            eventdata=alert['EventData'])

        # Record the new ticket id in the alert table
        record_ticket_id(ctx, ticket_id, alert['GUID'])


if __name__ == "__main__":
    lambda_handler('', '')
