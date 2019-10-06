import os

from twilio.rest import Client

from runners.helpers import log
from runners.helpers import vault


def handle(alert, type='sms', recipient_phone=None, sender_phone=None, message=None):

    if not os.environ.get('TWILIO_API_SID'):
        log.info(f"No TWILIO_API_SID in env, skipping handler.")
        return None

    twilio_sid = os.environ["TWILIO_API_SID"]

    twilio_token = vault.decrypt_if_encrypted(os.environ['TWILIO_API_TOKEN'])

    # check if phone is not empty if yes notification will be delivered to twilio
    if recipient_phone is None:
        log.error(f'Cannot identify assignee phone number')
        return None

    if message is None:
        log.error(f'SMS Message is empty')
        return None

    log.debug(
        f'Twilio message for recipient with phone number {recipient_phone}', message
    )

    client = Client(twilio_sid, twilio_token)

    response = client.messages.create(
        body=message, from_=sender_phone, to=recipient_phone
    )

    return response
