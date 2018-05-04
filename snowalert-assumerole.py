import snowflake.connector
import uuid
import json
import os
import boto3
import base64

def generate_alert(timestamp, severity, detector, environment, object_type, object, alert_type, description, event_data):
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
    alert['EventData'] = event_data

    return alert

def log_alerts(alerts):
    ctx.cursor().execute('use database snowalert')

    for alert in alerts:
        ctx.cursor().execute('insert into alerts (alert) select parse_json (column1) as variant from values (%s);', (alert))

def snowalert_query(ctx):
    detector = "SnowAlert-CloudTrail"

    query = """select
                  cloudtrail_phase_2.EVENT_TIME, 
                  case
                    when cloudtrail_phase_2.USER_IDENTITY_TYPE = 'IAMUser' then cloudtrail_phase_2.USER_IDENTITY_USERNAME
                    when cloudtrail_phase_2.USER_IDENTITY_TYPE = 'Root' then 'Root'
                    when cloudtrail_phase_2.USER_IDENTITY_TYPE = 'AssumedRole' then cloudtrail_phase_2.USER_IDENTITY_SESSION_CONTEXT_SESSION_ISSUER_USER_NAME
                    when cloudtrail_phase_2.USER_IDENTITY_TYPE = 'AWSAccount' then cloudtrail_phase_2.USER_IDENTITY_ACCOUNTID
                    when cloudtrail_phase_2.USER_IDENTITY_TYPE = 'AWSService' then cloudtrail_phase_2.USER_IDENTITY_INVOKEDBY
                  end AS "assume_role_source",
                    cloudtrail_phase_2.REQUEST_PARAMETERS:roleArn  AS "assumed_role",
                    cloudtrail_phase_2.RAW as "event_data"
                  from (
                    SELECT 
                        cloudtrail_phase_1.USER_IDENTITY_TYPE as cp1uit,
                      cloudtrail_phase_1.USER_IDENTITY_USERNAME as cp1uin,
                      cloudtrail_phase_1.USER_IDENTITY_SESSION_CONTEXT_SESSION_ISSUER_USER_NAME as cp1uasdf,
                      cloudtrail_phase_1.USER_IDENTITY_ACCOUNTID as cp1uia,
                      cloudtrail_phase_1.USER_IDENTITY_INVOKEDBY as cp1uii,
                        cloudtrail_phase_1.REQUEST_PARAMETERS:roleArn as cp1rpra
                    FROM sfc_dev_cloudtrail as cloudtrail_phase_1
                    WHERE cloudtrail_phase_1.EVENT_NAME = 'AssumeRole'
                    GROUP BY 1,2,3,4,5,6
                    HAVING COUNT(*) = 1 )
                inner join sfc_dev_cloudtrail as cloudtrail_phase_2 on
                  cp1uit = cloudtrail_phase_2.USER_IDENTITY_TYPE
                  and (
                        (cp1uit = 'IAMUser' and cp1uin = cloudtrail_phase_2.user_identity_username and cp1uin is not null)
                    or  (cp1uit = 'AssumedRole' and cp1uasdf = cloudtrail_phase_2.USER_IDENTITY_SESSION_CONTEXT_SESSION_ISSUER_USER_NAME and cp1uasdf is not null)
                    or  (cp1uit = 'AWSAccount' and cp1uia = cloudtrail_phase_2.USER_IDENTITY_ACCOUNTID and cp1uia is not null)
                    or  (cp1uii = 'AWSService' and cp1uii = cloudtrail_phase_2.USER_IDENTITY_INVOKEDBY and cp1uii is not null)
                      )
                  and cp1rpra = cloudtrail_phase_2.REQUEST_PARAMETERS:roleArn
                where
                ((((cloudtrail_phase_2.EVENT_TIME ) >= ((DATEADD('hour', -23, DATE_TRUNC('hour', CURRENT_TIMESTAMP())))) AND (cloudtrail_phase_2.EVENT_TIME ) < ((DATEADD('hour', 24, DATEADD('hour', -23, DATE_TRUNC('hour', CURRENT_TIMESTAMP()))))))));"""

    ctx.cursor().execute('use warehouse snowalert')
    ctx.cursor().execute('use database "sfc-dev-snow-35458"')

    results = ctx.cursor().execute(query).fetchall()
    alerts = []
    for res in results:
        alert = generate_alert(timestamp=str(res[0]),
                           severity=severity,
                           detector=detector,
                           environment="AWS",
                           object_type="IAM Role",
                           object=str(res[2]),
                           alert_type="First Time Assumed Role",
                           description=description.format(res[1], res[2]),
                           event_data=res[3])
        alerts.append(json.dumps(alert))

    log_alerts(alerts)

kms = boto3.client('kms')
encrypted_auth = os.environ['auth']
binary_auth = base64.b64decode(encrypted_auth)
decrypted_auth = kms.decrypt(CiphertextBlob = binary_auth)
auth = decrypted_auth['Plaintext'].decode()
auth = auth[:-1] # a newline or whitespace character is appended to the plaintext password, whoops.

description = "Source entity {0} assumed role {1} for the first time"
severity = 4


def lambda_handler(event, context):
    ctx = snowflake.connector.connect(
        user='snowalert',
        account='oz03309',
        password=auth
    )

    snowalert_query(ctx)