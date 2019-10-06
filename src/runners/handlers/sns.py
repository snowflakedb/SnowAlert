import json
import boto3

from botocore.exceptions import ClientError
from runners.helpers import log
from runners.helpers.dbconfig import REGION


def handle(
    alert,
    type='sns',
    topic=None,
    target=None,
    recipient_phone=None,
    subject=None,
    message_structure=None,
    message=None,
):

    # check if phone is nit empty if yes notification will be delivered to twilio
    if recipient_phone is None and topic is None and target is None:
        log.error(f'Cannot identify recipient')
        return None

    if message is None:
        log.error(f'SNS Message is empty')
        return None

    log.debug(f'SNS message ', message)

    client = boto3.client('sns', region_name=REGION)

    params = {}

    if message_structure is not None:
        params['MessageStructure'] = message_structure
        if message_structure == 'json':
            message = json.dumps(message)

    if topic is not None:
        params['TopicArn'] = topic
    if target is not None:
        params['TargetArn'] = target
    if recipient_phone is not None:
        params['PhoneNumber'] = recipient_phone
    if subject is not None:
        params['Subject'] = subject

    log.debug(f"SNS message", message)

    params['Message'] = message

    # Try to send the message.
    try:
        # Provide the contents of the message.
        response = client.publish(**params)
    # Display an error if something goes wrong.
    except ClientError as e:
        log.error(f'Failed to send message {e}')
        return None
    else:
        log.debug("SNS message sent!")

    return response
