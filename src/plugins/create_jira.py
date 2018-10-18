import os
from base64 import b64decode
import boto3
import yaml

import config
from helpers import log
from jira import JIRA

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
Description: {{quote}}{description}{{quote}}
Detector: {detector}
Event Data: {{code}}{event_data}{{code}}
Severity: {severity}
"""


def escape_jira_strings(v):
    if type(v) is str:
        return v.replace(r"{", r"\{")
    if type(v) is list:
        return [escape_jira_strings(x) for x in v]
    return v


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

    project = os.environ.get('JIRA_PROJECT', '')
    user = os.environ['JIRA_USER']
    jira = JIRA(os.environ.get('JIRA_URL', ''), basic_auth=(user, password))

    try:
        event_data = yaml.dump(event_data, indent=4, default_flow_style=False)
    except Exception as e:
        log.error("Error while creating ticket", e)

    # in JIRA ticket body, "{" is special symbol that breaks formatting
    escaped_locals_strings = {k: escape_jira_strings(v) for k, v in locals().items()}
    body = JIRA_TICKET_BODY_FMT.format(**escaped_locals_strings)
    body = body[:99000]

    print('Creating new JIRA ticket for', title, 'in project', project)
    new_issue = jira.create_issue(project=project,
                                  issuetype={'name': 'Story'},
                                  summary=title,
                                  description=body)

    return new_issue
