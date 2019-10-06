from os import environ
from urllib.parse import quote
import yaml
import os

from jira import JIRA

from runners.helpers import log, vault, db

PROJECT = environ.get('JIRA_PROJECT', '')
URL = environ.get('JIRA_URL', '')

JIRA_TICKET_BODY_DEFAULTS = {
    "DETECTOR": "No detector identified",
    "QUERY_NAME": "Query Name unspecified",
    "ENVIRONMENT": "No Environment described",
    "TITLE": "Untitled Query",
    "DESCRIPTION": "No Description provided",
    "SEVERITY": "Severity Unspecified",
}

JIRA_TICKET_BODY_FMT = """
Alert ID: {ALERT_ID}
Query ID: {QUERY_ID}
Query Name: {QUERY_NAME}
Environment: {ENVIRONMENT}
Sources: {SOURCES}
Actor: {ACTOR}
Object: {OBJECT}
Action: {ACTION}
Title: {TITLE}
Event Time: {EVENT_TIME}
Alert Time: {ALERT_TIME}
Description: {{quote}}
{DESCRIPTION}
{{quote}}
Detector: {DETECTOR}
Event Data: {{code}}{EVENT_DATA}{{code}}
Severity: {SEVERITY}
"""

password = vault.decrypt_if_encrypted(environ.get('JIRA_PASSWORD'))
user = environ.get('JIRA_USER')

if user and password:
    jira = JIRA(URL, basic_auth=(user, password))


def jira_ticket_body(alert):
    alert['SOURCES'] = ', '.join(alert['SOURCES'])
    escaped_locals_strings = {k: escape_jira_strings(v) for k, v in alert.items()}
    sources = escaped_locals_strings['SOURCES']
    escaped_locals_strings[
        'SOURCES'
    ] = f'[{sources}|{link_search_todos(f"Sources: {sources}")}]'
    jira_body = {**JIRA_TICKET_BODY_DEFAULTS, **escaped_locals_strings}
    ticket_body = JIRA_TICKET_BODY_FMT.format(**jira_body)
    return ticket_body[:99000]


def escape_jira_strings(v):
    """in JIRA ticket body, "{" and "[" are special symbols that need to be escaped"""
    if type(v) is str:
        return v.replace(r"{", r"\{").replace(r"[", r"\[")
    if type(v) is list:
        return [escape_jira_strings(x) for x in v]
    return escape_jira_strings(str(v))


def append_to_body(id, alert):
    if not user:
        return
    issue = jira.issue(id)
    description = get_ticket_description(issue)
    log.info(f"Appending data to ticket {id}")
    description = description + "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
    alert['SOURCES'] = ', '.join(alert['SOURCES'])
    alert['EVENT_DATA'] = yaml.dump(
        alert['EVENT_DATA'], indent=4, default_flow_style=False
    )
    escaped_locals_strings = {k: escape_jira_strings(v) for k, v in alert.items()}
    sources = escaped_locals_strings['SOURCES']
    escaped_locals_strings[
        'SOURCES'
    ] = f'[{sources}|{link_search_todos(f"SOURCES: {sources}")}]'
    jira_body = {**JIRA_TICKET_BODY_DEFAULTS, **escaped_locals_strings}
    description = description + JIRA_TICKET_BODY_FMT.format(**jira_body)

    issue.update(description=description)


def link_search_todos(description=None):
    q = f'project = {PROJECT} ORDER BY created ASC'

    if description:
        q = f'description ~ "{description}" AND {q}'

    return f'{URL}/issues/?jql={quote(q)}'


def create_jira_ticket(alert, assignee=None, custom_field=None):
    if not user:
        return

    try:
        alert['EVENT_DATA'] = yaml.dump(
            alert['EVENT_DATA'], indent=4, default_flow_style=False
        )
    except Exception as e:
        log.error("Error while creating ticket", e)

    body = jira_ticket_body(alert)

    log.info(f'Creating new JIRA ticket for "{alert["TITLE"]}" in project {PROJECT}')

    issue_params = {
        'project': PROJECT,
        'issuetype': {'name': 'Story'},
        'summary': alert['TITLE'],
        'description': body,
    }

    env_fields = os.environ.get('SA_JIRA_CUSTOM_FIELDS')
    if env_fields or custom_field:
        custom_fields = [f.split('=') for f in env_fields.split(';')]
        for field_id, field_value in custom_fields:
            issue_params[f'customfield_{field_id}'] = {'value': field_value}

        if custom_field:
            issue_params[f'customfield_{custom_field["id"]}'] = {
                'value': custom_field['value']
            }

    new_issue = jira.create_issue(**issue_params)

    if assignee:
        jira.assign_issue(new_issue, assignee)

    return new_issue


def check_ticket_status(id):
    status = str(jira.issue(id).fields.status)
    log.info(f"Ticket {id} status is {status}")
    return str(status)


def get_ticket_description(id):
    if not user:
        return
    return jira.issue(id).fields.description


def set_issue_done(issueId):
    return jira.transition_issue(issueId, 'done')


def record_ticket_id(ticket_id, alert_id):
    query = f"UPDATE results.alerts SET ticket='{ticket_id}' WHERE alert:ALERT_ID='{alert_id}'"
    print('Updating alert table:', query)
    try:
        db.execute(query)
    except Exception as e:
        log.error(e, f"Failed to update alert {alert_id} with ticket id {ticket_id}")


def bail_out(alert_id):
    query = f"UPDATE results.alerts SET handled='no handler' WHERE alert:ALERT_ID='{alert_id}'"
    print('Updating alert table:', query)
    try:
        db.execute(query)
    except Exception as e:
        log.error(e, f"Failed to update alert {alert_id} with handler status")


def handle(alert, correlation_id, project=PROJECT, assignee=None, custom_field=None):
    global PROJECT
    PROJECT = project
    if PROJECT == '':
        return "No Jira Project defined"
    if URL == '':
        return "No Jira URL defined."

    CORRELATION_QUERY = f"""
    SELECT *
    FROM results.alerts
    WHERE correlation_id = '{correlation_id}'
      AND iff(alert:HANDLERS is null, ticket is not null, handled is not null)
    ORDER BY EVENT_TIME DESC
    LIMIT 1
    """
    alert_id = alert['ALERT_ID']

    # We check against the correlation ID for alerts in that correlation with the same ticket
    correlated_results = list(db.fetch(CORRELATION_QUERY)) if correlation_id else []
    log.info(f"Discovered {len(correlated_results)} correlated results")

    if len(correlated_results) > 0:
        # There is a correlation with a ticket that exists, so we should append to that ticket
        ticket_id = correlated_results[0]['TICKET']
        try:
            ticket_status = check_ticket_status(ticket_id)
        except Exception:
            log.error(f"Failed to get ticket status for {ticket_id}")
            return

        if ticket_status == 'To Do':
            try:
                append_to_body(ticket_id, alert)
            except Exception as e:
                log.error(
                    f"Failed to append alert {alert_id} to ticket {ticket_id}.", e
                )
                try:
                    ticket_id = create_jira_ticket(alert)
                except Exception as e:
                    log.error(e, f"Failed to create ticket for alert {alert_id}")
                    return e
    else:
        # There is no correlation with a ticket that exists
        # Create a new ticket in JIRA for the alert
        try:
            ticket_id = create_jira_ticket(alert, assignee, custom_field)
        except Exception as e:
            log.error(e, f"Failed to create ticket for alert {alert_id}")
            return e

    record_ticket_id(ticket_id, alert_id)
    return ticket_id
