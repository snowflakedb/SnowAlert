import os
from jira import JIRA
import boto3
import base64
from time import sleep


def create_jira_ticket(guid, alertTime, severity, detector, env, objectType, object, alertType, description):
    if os.environ.get('PROD_FLAG'):
        kms = boto3.client('kms')
        encrypted_auth = os.environ['JIRA_API_PASSWORD']
        binary_auth = base64.b64decode(encrypted_auth)
        decrypted_auth = kms.decrypt(CiphertextBlob = binary_auth)
        auth = decrypted_auth['Plaintext'].decode()
        auth = auth[:-1] # The password has a newline on the end of it, so we chomp that off
    else:
        auth = os.environ['JIRA_API_PASSWORD']

    project = os.environ.get('SNOWALERT_JIRA_PROJECT', '') 
    user = os.environ.get('JIRA_API_USER', '')
    password = auth
    jira = JIRA(os.environ.get('SNOWALERT_JIRA_URL', ''), basic_auth=(user, password))

    body = \
        "GUID: " + guid + "\n" \
        "\nAlertTime: " + str(alertTime) + "\n" \
        "\nSeverity: " + str(severity) + " \n" \
        "\nDetector: " + str(detector) + " \n" \
        "\nAffectedEnv: " + str(env) + " \n" \
        "\nAffectedObjectType: " + str(objectType) + " \n" \
        "\nAffectedObject: " + str(object) + " \n" \
        "\nAlertType: " + str(alertType) + " \n" \
        "\nDescription: " + str(description)

    if os.environ.get('PROD_FLAG'):
        print('Creating new JIRA ticket for', alertType, 'in project', project)
        new_issue = jira.create_issue(project=project,
                                      issuetype={'name': 'Story'},
                                      summary=alertType,
                                      description=body)
    else:
        sleep(3)
        print('In prod would have created new JIRA ticket for', alertType, 'in project', project)
        new_issue = ''


    return new_issue

