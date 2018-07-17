import sys
import datetime
import json
import os
import uuid
import hashlib

import snowflake.connector

from multiprocessing import Process

from adapters.AwsLambda.main import AwsLambda


GROUPING_PERIOD = 1 * 60 * 60  # Group events within one hour periods
ALERTS_TABLE = 'snowalert.public.alerts'

# Grab alerts from past grouping_period amount of time 
def get_recent_alerts(ctx, alert_type):
    alert_map = {}
    recent_alerts = ctx.cursor().execute("select alert, counter from "+ALERTS_TABLE+" where try_cast(alert:EventTime::string as timestamp_ntz) >= \
                        ((DATEADD('second', (-1 * " + str(GROUPING_PERIOD) + "), CURRENT_TIMESTAMP()))) and alert:AlertType = '" + alert_type + "';").fetchall()
    for alert in recent_alerts:
        current_alert = json.loads(alert[0])
        key = hashlib.md5((current_alert['AffectedObject'] + current_alert['Description']).encode('utf-8')).hexdigest()
        alert_map[key] = [current_alert, alert[1], False]
    return alert_map


# Check if the proposed alert was already created recently, and update its counter
def alert_exists(alert_map, new_alert):
    key = hashlib.md5((new_alert['AffectedObject'] + new_alert['Description']).encode('utf-8')).hexdigest()
    if key in alert_map:
        alert_map[key][1] = alert_map[key][1] + 1
        alert_map[key][2] = True
        return True
    else:
        alert_map[key] = [new_alert, 1, False]
        return False


def update_table(ctx, alert):
    while True:
        try:
            ctx.cursor().execute("update " + ALERTS_TABLE + " set counter = " + str(alert[1]) + " where alert:GUID = '" + alert[0]['GUID'] + "';")
            sys.exit(0)
        except Exception as e:
            print("Alert Counter Update failed for alert: {}".format(alert[0]['GUID']))

# After checking all recent alerts, update counter for alerts that were duplicated
def update_recent_alerts(ctx, alert_map):
    for key in alert_map:
        if alert_map[key][2]:
            while True:
                try:
                    process = Process(target=update_table, args=(ctx, alert_map[key]))
                    process.start()
                    break
                except Exception as e:        
                    print("Process failed to fork, retrying.")


def generate_alert(query_spec, result):
    # Copy values from event to alert
    alert = dict.fromkeys(
        ['EventTime', 'Severity', 'Detector', 'AffectedEnv', 'AffectedObjectType', 'AffectedObject',
         'AlertType', 'Description', 'EventData'])
    for key in alert:
        alert[key] = alert_value_generator(key, query_spec, result)

    # Set alert values that aren't from the event
    if alert['EventTime'][-3:-2] == ":":
        alert['EventTime'] = alert['EventTime'][:-3] + alert['EventTime'][-2:]
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
    if len(alerts):
        format_string = ", ".join(["(%s)"]*len(alerts))
        ctx.cursor().execute('insert into '+ALERTS_TABLE+' (alert) select parse_json (column1) from values ' + format_string + ';', (alerts))


def get_snowflake_connection():
    pkb = AwsLambda.get_key()

    try:
        ctx = snowflake.connector.connect(user='snowalert', account=os.environ['account'], private_key=pkb)
    except Exception as e:
        print("Failed to authenticate with error {}".format(e))
        sys.exit(1)

    return ctx


def snowalert_query(event):
    query_spec = event
    print("Received query {}".format(query_spec['GUID']))

    ctx = get_snowflake_connection()

    try:
        results = ctx.cursor().execute(query_spec['Query']).fetchall()
    except Exception as e:
        print("Query {} execution failed. Error: {}".format(query_spec['GUID'], e))
        sys.exit(1)

    print("Query {} executed".format(query_spec['GUID']))
    alerts = []
    recent_alerts = get_recent_alerts(ctx, query_spec['AlertType'][0])
    for res in results:
        alert = generate_alert(query_spec, res)
        if alert_exists(recent_alerts, alert):
            try:
                print('Alert already exists, incrementing counter and passing over event: \n{}'.format(json.dumps(alert, indent=4)))
            except Exception as e:
                print('Unserializable JSON:\n{}'.format(alert))
        else:
            try:
                creation_time = datetime.datetime.strptime(alert['EventTime'],"%Y-%m-%d %H:%M:%S%z")
            except Exception as e:
                creation_time = datetime.datetime.strptime(alert['EventTime'],"%Y-%m-%d %H:%M:%S.%f%z")

            try:
                current_time = datetime.datetime.now(datetime.timezone.utc)
                delta = (current_time - creation_time).seconds
            except Exception as e:
                print("Query {} execution failed while setting creation_time.\nAlert contents:\n{}\nError: {}".format(query_spec['GUID'], alert, e))
                sys.exit(1)
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
    update_recent_alerts(ctx, recent_alerts)


def lambda_handler(event, context):
    snowalert_query(event)

