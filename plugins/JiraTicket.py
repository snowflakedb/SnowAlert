import os
from jira import JIRA


def create_jira_ticket(guid, creationTime, severity, detector, env, objectType, object, alertType, description):
    project = 'SA'
    user = os.environ.get('JIRA_API_USER', '')
    password = os.environ.get('JIRA_API_PASSWORD', '')
    jira = JIRA('https://snowflakecomputing.atlassian.net', basic_auth=(user, password))

    body = \
        "GUID: " + guid + "\n" \
        "\nCreationTime:" + str(creationTime) + "\n" \
        "\nSeverity: " + str(severity) + " \n" \
        "\nDetector: " + str(detector) + " \n" \
        "\nAffectedEnv: " + str(env) + " \n" \
        "\nAffectedObjectType: " + str(objectType) + " \n" \
        "\nAffectedObject: " + str(object) + " \n" \
        "\nAlertType: " + str(alertType) + " \n" \
        "\nDescription: " + str(description)

    new_issue = jira.create_issue(project=project,
                                  issuetype={'name': 'Story'},
                                  summary=alertType,
                                  description=body)
    return new_issue

