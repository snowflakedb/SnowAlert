import os
import json

from slackclient import SlackClient

from runners.helpers import log
from runners.helpers import db
from runners.helpers import vault

API_TOKEN = os.environ.get('SA_SLACK_API_TOKEN', os.environ.get('SLACK_API_TOKEN'))


def message_template(vars):
    payload = None

    # remove handlers data, it might contain JSON incompatible strucutres
    vars['alert'].pop('HANDLERS')

    # if we have Slack user data, send it to template
    if 'user' in vars:
        params = {
            'alert': vars['alert'],
            'properties': vars['properties'],
            'user': vars['user'],
        }
    else:
        params = {'alert': vars['alert'], 'properties': vars['properties']}

    log.debug(f"Javascript template parameters", params)
    try:
        # retrieve Slack message structure from javascript UDF
        rows = db.connect_and_fetchall(
            "select " + vars['template'] + "(parse_json(%s))",
            params=[json.dumps(params)],
        )
        row = rows[1]

        if len(row) > 0:
            log.debug(f"Template {vars['template']}", ''.join(row[0]))
            payload = json.loads(''.join(row[0]))
        else:
            log.error(f"Error loading javascript template {vars['template']}")
            raise Exception(f"Error loading javascript template {vars['template']}")
    except Exception as e:
        log.error(f"Error loading javascript template", e)
        raise

    log.debug(f"Template payload", payload)
    return payload


def handle(
    alert,
    recipient_email=None,
    channel=None,
    template=None,
    message=None,
    file_content=None,
    file_type=None,
    file_name=None,
    blocks=None,
    attachments=None,
    api_token=API_TOKEN,
    slack_api_token=None,
):
    slack_token_ct = slack_api_token or api_token
    slack_token = vault.decrypt_if_encrypted(slack_token_ct)
    sc = SlackClient(slack_token)

    # otherwise we will retrieve email from assignee and use it to identify Slack user
    # Slack user id will be assigned as a channel

    title = alert['TITLE']

    if recipient_email is not None:
        if isinstance(recipient_email, str):
            if recipient_email is not None:
                result = sc.api_call("users.lookupByEmail", email=recipient_email)

            # log.info(f'Slack user info for {email}', result)

            if result['ok'] is True and 'error' not in result:
                user = result['user']
                channel_id = user['id']
            else:
                log.error(f'Cannot identify  Slack user for email {recipient_email}')
                return None

        elif isinstance(recipient_email, list):
            users = []
            for email in recipient_email:
                response = sc.api_call("users.lookupByEmail", email=email)
                if not response['ok']:
                    log.error(f'Cannot identify Slack user for email {email}')
                    continue
                users.append(response['user']['id'])
            user_ids = ",".join(users)
            result = sc.api_call("conversations.open", users=user_ids)
            if result['ok']:
                channel_id = result['channel']['id']
            else:
                raise RuntimeError(f"Error ocurred while opening conversation channel")

    # check if channel exists, if yes notification will be delivered to the channel
    if channel is not None:
        log.info(f'Creating new SLACK message for {title} in channel', channel)
    else:
        if recipient_email is not None:
            channel = channel_id
            log.info(
                f'Creating new SLACK message for {title} for user {recipient_email}'
            )
        else:
            raise RuntimeError("missing both 'channel' and 'recipient_email' param")

    text = title

    if template is not None:
        properties = {'channel': channel, 'message': message}

        # create Slack message structure in Snowflake javascript UDF
        payload = message_template(locals())

        if payload is not None:
            if 'blocks' in payload:
                blocks = json.dumps(payload['blocks'])

            if 'attachments' in payload:
                attachments = json.dumps(payload['attachments'])

            if 'text' in payload:
                text = payload['text']
        else:
            raise RuntimeError(f"Payload is empty for template {template}")

    else:
        # does not have template, will send just simple message
        if message is not None:
            text = message

    response = None

    if file_content is not None:
        if template is not None:
            response = sc.api_call(
                "chat.postMessage",
                channel=channel,
                text=text,
                blocks=blocks,
                attachments=attachments,
            )

        file_descriptor = sc.api_call(
            "files.upload",
            content=file_content,
            title=text,
            channels=channel,
            iletype=file_type,
            filename=file_name,
        )

        if file_descriptor['ok'] is True:
            file = file_descriptor["file"]
            file_url = file["url_private"]
        else:
            log.error(f"Slack file upload error", file_descriptor['error'])

    else:
        response = sc.api_call(
            "chat.postMessage",
            channel=channel,
            text=text,
            blocks=blocks,
            attachments=attachments,
        )

    if response is not None:
        log.debug(f'Slack response', response)

        if response['ok'] is False:
            raise RuntimeError(f"Slack handler error {response['error']}")

        if 'message' in response:
            del response['message']

    return response
