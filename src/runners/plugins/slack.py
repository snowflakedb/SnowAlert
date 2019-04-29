import os
import json

from slackclient import SlackClient

from runners.helpers import log
from runners.helpers import db
from runners.helpers import kms


def message_template(vars):
    payload = None

    # if we have Slack user data, send it to template
    if 'user' in vars:
        params = {'alert': vars['alert'], 'properties': vars['properties'], 'user': vars['user']}
    else:
        params = {'alert': vars['alert'], 'properties': vars['properties']}

    try:
        # retrieve Slack message structure from javascript UDF
        rows = db.connect_and_fetchall(
            "select " + vars['template'] + "(parse_json('" + json.dumps(params) + "'))")
        row = rows[1]

        if len(row) > 0:
            log.debug(f"Template {vars['template']}", ''.join(row[0]))
            payload = json.loads(''.join(row[0]))
        else:
            log.error(f"Error loading javascript template {vars['template']}")
            raise Exception("Error loading javascript template " + {vars['template']})
    except Exception as e:
        log.error(f"Error loading javascript template", e)
        raise

    return payload


def record_status(response, alert_id):
    query = f"UPDATE results.alerts SET handled=%s WHERE alert:ALERT_ID='{alert_id}'"
    print('Updating alert table:', query)
    try:

        db.connect().cursor().execute(query, [str(response)])  # TODO: add safe UPDATE to db
    except Exception as e:
        log.error(e, f"Failed to update alert {alert_id} with status {response}")


def handle(alert_shell, type='slack', recipient_email=None, channel=None, template=None, message=None):
    alert = alert_shell['ALERT']
    if not os.environ.get('SLACK_API_TOKEN'):
        log.info(f"No SLACK_API_TOKEN in env, skipping handler.")
        return None

    slack_token = os.environ["SLACK_API_TOKEN"]

    if len(slack_token) > 100:  # then it must be encrypted!
        try:
            slack_token = kms.decrypt_if_encrypted(slack_token, handleErrors=False)
        except Exception as e:
            log.error(f"Error decrypting Slack token", e)
            raise

    sc = SlackClient(slack_token)

    # otherwise we will retrieve email from assignee and use it to identify Slack user
    # Slack user id will be assigned as a channel

    title = alert['TITLE']

    if recipient_email is not None:
        result = sc.api_call("users.lookupByEmail", email=recipient_email)

        # log.info(f'Slack user info for {email}', result)

        if result['ok'] is True and 'error' not in result:
            user = result['user']
            userid = user['id']
        else:
            log.error(f'Cannot identify  Slack user for email {recipient_email}')
            raise Exception('Cannot identify  Slack user for email ' + {recipient_email})

    # check if channel exists, if yes notification will be delivered to the channel
    if channel is not None:
        log.info(f'Creating new SLACK message for {title} in channel', channel)
    else:
        if 'recipient_email' is not None:
            channel = userid
            log.info(f'Creating new SLACK message for {title} for user {recipient_email}')
        else:
            log.error(f'Cannot identify assignee email')
            raise Exception('Cannot identify assignee email')

    blocks = None
    attachments = None
    text = title

    if template is not None:
        properties = {'channel': channel, 'message': message}

        # create Slack message structure in Snowflake javascript UDF
        try:
            payload = message_template(locals())
        except Exception as e:
            raise e

        if payload is not None:
            if 'blocks' in payload:
                blocks = json.dumps(payload['blocks'])

            if 'attachments' in payload:
                attachments = json.dumps(payload['attachments'])

            if 'text' in payload:
                text = payload['text']
        else:
            raise Exception('Payload is empty for template ' + {template})
    else:
        # does not have template, will send just simple message
        if 'message' is not None:
            text = message

    response = sc.api_call(
        "chat.postMessage",
        channel=channel,
        text=text,
        blocks=blocks,
        attachments=attachments
    )

    log.debug(f'Slack response', response)

    if response['ok'] is False:
        log.error(f"Slack handler error", response['error'])
        raise Exception('Slack handler error', response['error'])

    if 'message' in response:
        del response['message']

    record_status(response, alert['ALERT_ID'])

    return response
