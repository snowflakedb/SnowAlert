import os
from jira import JIRA
import boto3
import base64

def create_jira_ticket(guid, creationTime, severity, detector, env, objectType, object, alertType, description):
    kms = boto3.client('kms')
    encrypted_auth = os.environ['JIRA_API_PASSWORD']
    binary_auth = base64.b64decode(encrypted_auth)
    decrypted_auth = kms.decrypt(CiphertextBlob = binary_auth)
    auth = decrypted_auth['Plaintext'].decode()
    auth = auth[:-1] # The password has a newline on the end of it, so we chomp that off

    project = 'SA'
    user = os.environ.get('JIRA_API_USER', '')
    password = auth
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

