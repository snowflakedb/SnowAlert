import os
import json
import snowflake.connector
import plugins


def db_connect():
    connection = snowflake.connector.connect(
        account=os.environ['SNOWALERT_ACCOUNT'],
        user=os.environ['SNOWALERT_USER'],
        password=os.environ['SNOWALERT_PASSWORD']
    )
    connection.cursor().execute('use warehouse snowalert')
    connection.cursor().execute('use database snowalert')
    return connection


def get_new_alerts(connection):
    alert_query = 'select * from alerts where ticket is null'

    results = connection.cursor().execute(alert_query).fetchall()
    return results


def record_ticket_id(connection, ticket_id, guid):
    query = "update alerts set ticket = '" + str(ticket_id) + "' where alert:GUID = '" + str(guid) + "'"
    print('debug:', query)
    connection.cursor().execute(query)


if __name__ == "__main__":
    ctx = db_connect()

    for row in get_new_alerts(ctx):
        alert = json.loads(row[0])

        # Create a new ticket in JIRA for the alert
        ticketID = plugins.create_jira_ticket(
            guid=alert['GUID'],
            creationTime=alert['CreationTime'],
            severity=alert['Severity'],
            detector=alert['Detector'],
            env=alert['AffectedEnv'],
            objectType=alert['AffectedObjectType'],
            object=alert['AffectedObject'],
            alertType=alert['AlertType'],
            description=alert['Description'])

        # Record the new ticket id in the alert table
        record_ticket_id(ctx, ticketID, alert['GUID'])
