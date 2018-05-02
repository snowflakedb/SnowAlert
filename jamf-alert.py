import snowflake.connector
import uuid
import json
import datetime
import os


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


def insert_alert(alert):
    query = ""


ctx = snowflake.connector.connect(
    user=os.environ['SNOWALERTS_USER'],
    account='oz03309',
    password=os.environ['SNOWALERTS_PASSWORD']
)

sip_disabled_description = "The affected computer has System Integrity Protection turned off."
sip_severity = 3

detector = "Jamf"

sip_query = 'select v:timestamp::timestamp_tz as time, ' \
            'r.value:name::string as name, ' \
            'r.value:id::int as id ' \
            'from (' \
            'select v from jamf.public.jamf order by v:timestamp::timestamp_tz desc limit 1) j,' \
            'lateral flatten (input => v:"SIP Disabled":computer_group:computers) r;'

ctx.cursor().execute('use warehouse snowalerts')
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
    alerts.append(alert)

for i in alerts:
    print(i)
