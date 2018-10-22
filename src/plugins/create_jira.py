import boto3
from base64 import b64decode
import os
from urllib.parse import quote
import yaml

import config
from helpers import log
from jira import JIRA

PROJECT = os.environ.get('JIRA_PROJECT', '')
URL = os.environ.get('JIRA_URL', '')

JIRA_TICKET_BODY_FMT = """
Alert ID: {alert_id}
Query ID: {query_id}
Query Name: {query_name}
Environment: {environment}
Sources: {sources}
Actor: {actor}
Object: {object}
Action: {action}
Title: {title}
Event Time: {event_time}
Alert Time: {alert_time}
Description: {{quote}}
{description}
{{quote}}
Detector: {detector}
Event Data: {{code}}{event_data}{{code}}
Severity: {severity}
"""


def jira_ticket_body(vars):
    sources = ', '.join(vars['sources'])

    escaped_locals_strings = {k: escape_jira_strings(v) for k, v in vars.items()}
    vars['sources'] = f'[{sources}|{link_search_todos(f"Sources: {sources}")}]'

    body = JIRA_TICKET_BODY_FMT.format(**escaped_locals_strings)
    return body[:99000]


def escape_jira_strings(v):
    """in JIRA ticket body, "{" and "[" are special symbols that need to be escaped"""
    if type(v) is str:
        return v.replace(r"{", r"\{").replace(r"[", r"\[")
    if type(v) is list:
        return [escape_jira_strings(x) for x in v]
    return escape_jira_strings(str(v))


def link_search_todos(description=None):
    q = f'project = {PROJECT} AND status = "to do" ORDER BY created ASC'

    if description:
        q = f'description ~ "{description}" AND {q}'

    return f'{URL}/issues/?jql={quote(q)}'


def create_jira_ticket(alert_id, query_id, query_name, environment, sources, actor, object, action, title, event_time,
                       alert_time, description, detector, event_data, severity):
    kms = boto3.client('kms', region_name=config.REGION)
    encrypted_auth = os.environ['JIRA_PASSWORD']

    if len(encrypted_auth) < 100:  # then we treat it an an unencrypted password
        password = encrypted_auth
    else:
        kms = boto3.client('kms', region_name=config.REGION)
        binary_auth = b64decode(encrypted_auth)
        decrypted_auth = kms.decrypt(CiphertextBlob=binary_auth)
        password = decrypted_auth['Plaintext'].decode()

    user = os.environ['JIRA_USER']
    jira = JIRA(URL, basic_auth=(user, password))

    try:
        event_data = yaml.dump(event_data, indent=4, default_flow_style=False)
    except Exception as e:
        log.error("Error while creating ticket", e)

    body = jira_ticket_body(locals())

    print(f'Creating new JIRA ticket for {title} in project', PROJECT)
    new_issue = jira.create_issue(project=PROJECT,
                                  issuetype={'name': 'Story'},
                                  summary=title,
                                  description=body)

    return new_issue
