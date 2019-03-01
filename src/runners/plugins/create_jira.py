from base64 import b64decode
import boto3
import os
from urllib.parse import quote
import yaml

from jira import JIRA

from runners.helpers.dbconfig import REGION
from runners.helpers import log

PROJECT = os.environ.get('JIRA_PROJECT', '')
URL = os.environ.get('JIRA_URL', '')

JIRA_TICKET_BODY_DEFAULTS = {
    "DETECTOR": "No detector identified",
    "QUERY_NAME": "Query Name unspecified",
    "ENVIRONMENT": "No Environment described",
    "TITLE": "Untitled Query",
    "DESCRIPTION": "No Description provided",
    "SEVERITY": "Severity Unspecified"
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

kms = boto3.client('kms', region_name=REGION)
encrypted_auth = os.environ['JIRA_PASSWORD']

if len(encrypted_auth) < 100:  # then we treat it an an unencrypted password
    password = encrypted_auth
else:
    kms = boto3.client('kms', region_name=REGION)
    binary_auth = b64decode(encrypted_auth)
    decrypted_auth = kms.decrypt(CiphertextBlob=binary_auth)
    password = decrypted_auth['Plaintext'].decode()

user = os.environ['JIRA_USER']

jira = JIRA(URL, basic_auth=(user, password))


def jira_ticket_body(alert):
    alert['SOURCES'] = ', '.join(alert['SOURCES'])
    escaped_locals_strings = {k: escape_jira_strings(v) for k, v in alert.items()}
    sources = escaped_locals_strings['SOURCES']
    escaped_locals_strings['SOURCES'] = f'[{sources}|{link_search_todos(f"Sources: {sources}")}]'
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
    issue = jira.issue(id)
    description = get_ticket_description(issue)
    description = description + "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n"
    alert['SOURCES'] = ', '.join(alert['SOURCES'])
    alert['EVENT_DATA'] = yaml.dump(alert['EVENT_DATA'], indent=4, default_flow_style=False)
    escaped_locals_strings = {k: escape_jira_strings(v) for k, v in alert.items()}
    sources = escaped_locals_strings['SOURCES']
    escaped_locals_strings['SOURCES'] = f'[{sources}|{link_search_todos(f"SOURCES: {sources}")}]'
    jira_body = {**JIRA_TICKET_BODY_DEFAULTS, **escaped_locals_strings}
    description = description + JIRA_TICKET_BODY_FMT.format(**jira_body)

    issue.update(description=description)


def link_search_todos(description=None):
    q = f'project = {PROJECT} ORDER BY created ASC'

    if description:
        q = f'description ~ "{description}" AND {q}'

    return f'{URL}/issues/?jql={quote(q)}'


def create_jira_ticket(alert):
    try:
        alert['EVENT_DATA'] = yaml.dump(alert['EVENT_DATA'], indent=4, default_flow_style=False)
    except Exception as e:
        log.error("Error while creating ticket", e)

    body = jira_ticket_body(alert)

    print(f'Creating new JIRA ticket for {alert["TITLE"]} in project', PROJECT)
    new_issue = jira.create_issue(project=PROJECT,
                                  issuetype={'name': 'Story'},
                                  summary=alert['TITLE'],
                                  description=body)

    return new_issue


def check_ticket_status(id):
    return jira.issue(id).fields.status


def get_ticket_description(id):
    return jira.issue(id).fields.description
