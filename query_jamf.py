import snowflake.connector
import uuid
import json
import os
import boto3
import base64


def generate_alert(timestamp, severity, detector, environment, object_type, object, alert_type, description):
    alert = {}
    alert['GUID'] = uuid.uuid4().hex
    alert['CreationTime'] = timestamp
    alert['Severity'] = severity
    alert['Detector'] = detector
    alert['AffectedEnv'] = environment
    alert['AffectedObjectType'] = object_type
    alert['AffectedObject'] = object
    alert['AlertType'] = alert_type
    alert['Description'] = description

    return alert


def log_alerts(alerts):
    ctx.cursor().execute('use database snowalert')

    for alert in alerts:
        ctx.cursor().execute('insert into alerts (alert) select parse_json (column1) as variant from values (%s);', (alert))


# Look for members in the user-created "SIP Disabled" computer group within JAMF
def jamf_query():
    detector = "Jamf"

    sip_query = 'select v:timestamp::timestamp_tz as time, ' \
        'r.value:name::string as name, ' \
        'r.value:id::int as id ' \
        'from (' \
        'select v from jamf.public.jamf order by v:timestamp::timestamp_tz desc limit 1) j,' \
        'lateral flatten (input => v:"SIP Disabled":computer_group:computers) r;'

    ctx.cursor().execute('use warehouse snowalert')
    ctx.cursor().execute('use database jamf')

    results = ctx.cursor().execute(sip_query).fetchall()
    alerts = []
    for res in results:
        alert = generate_alert(timestamp=str(res[0]),
                           severity=sip_severity,
                           detector=detector,
                           environment="Endpoints",
                           object_type="Macbook",
                           object=str(res[1]) + " : " + str(res[2]),
                           alert_type="SIP Disabled",
                           description=sip_disabled_description)
        alerts.append(json.dumps(alert))

    log_alerts(alerts)


kms = boto3.client('kms')
encrypted_auth = os.environ['auth']
binary_auth = base64.b64decode(encrypted_auth)
decrypted_auth = kms.decrypt(CiphertextBlob = binary_auth)
auth = decrypted_auth['Plaintext'].decode()
auth = auth[:-1] # a newline or whitespace character is appended to the plaintext password, whoops.

sip_disabled_description = "The affected computer has System Integrity Protection turned off."
sip_severity = 3


ctx = snowflake.connector.connect(
        account=os.environ['SNOWALERT_ACCOUNT'],
        user=os.environ['SNOWALERT_USER'],
        password=str(auth)
)


def lambda_handler(event, context):
    jamf_query()
