from json import dumps
from os import environ
from urllib.parse import quote
import os

from jira import JIRA, User

from runners.helpers import log, vault, db
from runners.utils import yaml

PROJECT = environ.get('SA_JIRA_PROJECT', environ.get('JIRA_PROJECT', ''))
URL = environ.get('SA_JIRA_URL', environ.get('JIRA_URL', ''))
WEBUI_LINK = environ.get('SA_JIRA_WEBUI_URL', environ.get('JIRA_WEBUI_URL', ''))
TRIAGE_LINK = environ.get('SA_JIRA_TRIAGE_URL', environ.get('JIRA_TRIAGE_URL', ''))
ISSUE_TYPE = environ.get('SA_JIRA_ISSUE_TYPE', environ.get('JIRA_ISSUE_TYPE', 'Story'))
TODO_STATUS = environ.get(
    'SA_JIRA_STARTING_STATUS', environ.get('JIRA_STARTING_STATUS', 'To Do')
)

JIRA_TICKET_BODY_DEFAULTS = {
    "DETECTOR": "No detector identified",
    "QUERY_NAME": "Query Name unspecified",
    "ENVIRONMENT": "No Environment described",
    "TITLE": "Untitled Query",
    "DESCRIPTION": "No Description provided",
    "SEVERITY": "Severity Unspecified",
    "CATS": "-",
}

JIRA_TICKET_BODY_FMT = """
Alert ID: {ALERT_ID}
Query ID: {QUERY_ID}
Query Name: {QUERY_NAME}
Environment: {ENVIRONMENT}
Sources: {SOURCES}
Categories: {CATS}
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

password = vault.decrypt_if_encrypted(
    environ.get('SA_JIRA_API_TOKEN', environ.get('JIRA_API_TOKEN'))
    or environ.get('SA_JIRA_PASSWORD', environ.get('JIRA_PASSWORD'))
)
user = environ.get('SA_JIRA_USER', environ.get('JIRA_USER'))

jira_server = URL if URL.startswith('https://') else f'https://{URL}'

if user and password:
    jira = JIRA(jira_server, basic_auth=(user, password))


def jira_ticket_body(alert, project):
    sources = alert['SOURCES']
    alert['SOURCES'] = ', '.join(sources) if isinstance(sources, list) else sources
    escaped_locals_strings = {k: escape_jira_strings(v) for k, v in alert.items()}

    if WEBUI_LINK:
        query_id = alert['QUERY_ID']
        escaped_locals_strings['QUERY_ID'] = f'[{query_id}|{WEBUI_LINK.format(query_id)}]'

    if TRIAGE_LINK:
        query_name = alert['QUERY_NAME']
        escaped_locals_strings['QUERY_NAME'] = f'[{query_name}|{TRIAGE_LINK.format(query_name)}]'

    sources = escaped_locals_strings['SOURCES']
    escaped_locals_strings[
        'SOURCES'
    ] = f'[{sources}|{link_search_todos(f"Sources: {sources}", project)}]'

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


def append_to_body(id, alert, project):
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
    ] = f'[{sources}|{link_search_todos(f"SOURCES: {sources}", project)}]'
    jira_body = {**JIRA_TICKET_BODY_DEFAULTS, **escaped_locals_strings}
    description = description + JIRA_TICKET_BODY_FMT.format(**jira_body)

    issue.update(description=description)


def link_search_todos(description=None, project=PROJECT):
    q = f'project = {project} ORDER BY created DESC'

    if description:
        q = f'description ~ "{description}" AND {q}'

    return f'{jira_server}/issues/?jql={quote(q)}'


def create_jira_ticket(
    alert,
    assignee=None,
    custom_fields=None,
    project=PROJECT,
    issue_type=ISSUE_TYPE,
):
    if not user:
        return

    if not custom_fields:
        custom_fields = ''

    try:
        alert['EVENT_DATA'] = yaml.dump(
            alert['EVENT_DATA'], indent=4, default_flow_style=False
        )
    except Exception as e:
        log.error("Error while creating ticket", e)

    body = jira_ticket_body(alert, project)

    log.info(f'Creating new JIRA ticket for "{alert["TITLE"]}" in project {project}')

    issue_params = {
        'project': project,
        'issuetype': {'name': issue_type},
        'summary': alert.get('TITLE') or JIRA_TICKET_BODY_DEFAULTS['TITLE'],
        'description': body,
    }

    # combine fields from envar and from alert query definition
    # e.g.
    # envar_fields = '10008=key:SAD-11493;10009=Low'
    # custom_fields = '10009=Critical'
    # fields = ['10008=key:SAD-11493', '10009=Critical']
    envar_fields = os.environ.get('SA_JIRA_CUSTOM_FIELDS', '')
    fields = ';'.join(envar_fields.split(';') + custom_fields.split(';')).split(';')
    if fields:
        custom_fields = [f.split('=') for f in fields if f]
        for field_id, field_value in custom_fields:
            if field_value.startswith('key:'):
                issue_params[f'customfield_{field_id}'] = field_value[4:]
            elif field_value.startswith('[') and field_value.endswith(']'):
                issue_params[f'customfield_{field_id}'] = [{'value': v} for v in field_value[1:-1].split(',')]
            else:
                issue_params[f'customfield_{field_id}'] = {'value': field_value}

    new_issue = jira.create_issue(**issue_params)

    if assignee:
        # no longer works because of gdpr mode:
        # jira.assign_issue(new_issue, assignee)

        # temp work-around until Jira Python library supports gdpr mode better
        u = next(
            jira._fetch_pages(User, None, 'user/search', 0, 1, {'query': assignee})
        )
        jira._session.put(
            jira._options['server']
            + '/rest/api/latest/issue/'
            + str(new_issue)
            + '/assignee',
            data=dumps({'accountId': u.accountId}),
        )

    return new_issue


def check_ticket_status(id):
    status = str(jira.issue(id).fields.status)
    log.info(f"Ticket {id} status is '{status}'")
    return str(status)


def get_ticket_description(id):
    if not user:
        return
    return jira.issue(id).fields.description


def set_issue_done(issueId):
    return jira.transition_issue(issueId, 'done')


def record_ticket_id(ticket_id, alert_id):
    query = f"UPDATE results.alerts SET ticket='{ticket_id}' WHERE alert_id='{alert_id}'"
    print('Updating alert table:', query)
    try:
        db.execute(query)
    except Exception as e:
        log.error(e, f"Failed to update alert {alert_id} with ticket id {ticket_id}")


def handle(
    alert,
    correlation_id,
    project=PROJECT,
    assignee=None,
    custom_fields=None,
    issue_type=ISSUE_TYPE,
    jira_url=URL,
    starting_status=TODO_STATUS,
):
    if project == '':
        return "No Jira Project defined"
    if jira_url == '':
        return "No Jira URL defined."

    CORRELATION_QUERY = f"""
    SELECT *
    FROM results.alerts
    WHERE correlation_id = '{correlation_id}'
      AND ticket IS NOT NULL
    ORDER BY event_time DESC
    LIMIT 1
    """
    alert_id = alert['ALERT_ID']

    correlated_result = next(db.fetch(CORRELATION_QUERY), {}) if correlation_id else {}
    ticket_id = correlated_result.get('TICKET')

    if ticket_id:
        try:
            ticket_status = check_ticket_status(ticket_id)
        except Exception:
            ticket_id = None
            ticket_status = None
            log.error(f"Failed to get ticket status for {ticket_id}")

        if ticket_status == starting_status:
            try:
                append_to_body(ticket_id, alert, project)
            except Exception as e:
                ticket_id = None
                log.error(
                    f"Failed to append alert {alert_id} to ticket {ticket_id}.", e
                )

        else:
            ticket_id = None

    try:
        if ticket_id is None:
            ticket_id = create_jira_ticket(
                alert,
                assignee,
                custom_fields=custom_fields,
                project=project,
                issue_type=issue_type,
            )

    except Exception as e:
        log.error(e, f"Failed to create ticket for alert {alert_id}")
        raise

    record_ticket_id(ticket_id, alert_id)

    return ticket_id
