import os
from jira import JIRA


def create_jira_ticket(guid, creationTime, severity, detector, env, objectType, object, alertType, description):
    project = 'SA'
    user = os.environ.get('JIRA_API_USER', '')
    password = os.environ.get('JIRA_API_PASSWORD', '')
    jira = JIRA('https://snowflakecomputing.atlassian.net', basic_auth=(user, password))

    body = \
        "GUID: " + guid + "\n" \
        "\nCreationTime:" + creationTime + "\n" \
        "\nSeverity: " + severity + " \n" \
        "\nDetector: " + detector + " \n" \
        "\nAffectedEnv: " + env + " \n" \
        "\nAffectedObjectType: " + objectType + " \n" \
        "\nAffectedObject: " + object + " \n" \
        "\nAlertType: " + alertType + " \n" \
        "\nDescription: " + description

    new_issue = jira.create_issue(project=project,
                                  issuetype={'name': 'Story'},
                                  summary=alertType,
                                  description=body)
    return new_issue

