import sys
import base64
import boto3
import datetime
import json
import os
import uuid
import snowflake.connector

GROUPING_PERIOD = 1 * 60 * 60  # Group events within one hour periods
ALERTS_TABLE = 'snowalert.public.alerts'


# Check if the proposed alert was already created recently
def alert_exists(ctx, new_alert):
    matching_guid = ''

    # Get alerts created within the query period
    recent_alerts = ctx.cursor().execute('select * from '+ALERTS_TABLE+' where try_cast(alert:EventTime::string as timestamp_ntz) >= \
                            ((DATEADD(\'second\', (-1 * ' + str(
        GROUPING_PERIOD) + '), CURRENT_TIMESTAMP())));').fetchall()
    for res in recent_alerts:
        recent_alert = json.loads(res[0])

        # Check if recent alerts have matching field values as the potential new alert
        if new_alert.get('AffectedObject') == recent_alert.get('AffectedObject') and \
                new_alert.get('Description') == recent_alert.get('Description'):

            # Return the GUID of the alert that already exists so that we don't create a new one like it
            matching_guid = recent_alert.get('GUID')

    return matching_guid


# In cases where alert already exists, we don't create a new one but increment its counter
def increment_alert_counter(ctx, alert_guid):
    ctx.cursor().execute("update "+ALERTS_TABLE+" set counter = counter + 1 where  alert:GUID = \'"+alert_guid+"\';")


def generate_alert(query_spec, result):
    # Copy values from event to alert
    alert = dict.fromkeys(
        ['EventTime', 'Severity', 'Detector', 'AffectedEnv', 'AffectedObjectType', 'AffectedObject',
         'AlertType', 'Description', 'EventData'])
    for key in alert:
        alert[key] = alert_value_generator(key, query_spec, result)

    # Set alert values that aren't from the event
    alert['GUID'] = uuid.uuid4().hex
    alert['AlertTime'] = str(datetime.datetime.utcnow())
    alert['QueryGUID'] = query_spec['GUID']
    return alert


def alert_value_generator(alert_field, query_spec, result):
    try:
        return query_spec[alert_field][0].format(*[result[pos] for pos in query_spec[alert_field][1:]])
    except Exception as e:
        print("Missing required key {} in query {}. Error: {}".format(alert_field, query_spec['GUID'], e))
        sys.exit(1)


def log_alerts(ctx, alerts):
    for alert in alerts:
        ctx.cursor().execute(
            'insert into '+ALERTS_TABLE+' (alert) select parse_json (column1) as variant from values (%s);',
            alert)


def snowalert_query(event):
    query_spec = event
    print("Received query {}".format(query_spec['GUID']))
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
    try:
        results = ctx.cursor().execute(query_spec['Query']).fetchall()
    except Exception as e:
        print("Query {} execution failed. Error: {}".format(query_spec['GUID'], e))
        sys.exit(1)
    print("Query {} executed".format(query_spec['GUID']))
    alerts = []
    for res in results:
        alert = generate_alert(query_spec, res)
        existing_alert_guid = alert_exists(ctx, alert)
        if existing_alert_guid:
            print('Alert already exists, incrementing counter and passing over event:', res)
            increment_alert_counter(ctx, existing_alert_guid)
        else:
            creation_time = datetime.datetime.strptime(alert['EventTime'], "%Y-%m-%d %H:%M:%S")
            current_time = datetime.datetime.utcnow()
            delta = (current_time - creation_time).seconds
            if delta > 3600:
                    print(
                        "Query ID: {} Alert Creation Time: {} Lambda Current Time (UTC): {} Time Difference: {}".format(
                            query_spec['GUID'],
                            creation_time,
                            current_time,
                            delta))
                    print("Time between creation and current over an hour. Logging skipped for alert: \n{}".format(
                        json.dumps(alert, indent=4)))
            else:
                alerts.append(json.dumps(alert))
    log_alerts(ctx, alerts)


def lambda_handler(event, context):
    snowalert_query(event)

