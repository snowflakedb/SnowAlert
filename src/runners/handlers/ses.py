import boto3

from botocore.exceptions import ClientError
from runners.helpers import log
from runners.helpers.dbconfig import REGION


def handle(
    alert,
    type='ses',
    recipient_email=None,
    sender_email=None,
    text=None,
    html=None,
    subject=None,
    cc=None,
    bcc=None,
    reply_to=None,
    charset="UTF-8",
):

    # check if recipient email is not empty
    if recipient_email is None:
        log.error(f'Cannot identify recipient email')
        return None

    if text is None:
        log.error(f'SES Message is empty')
        return None

    if cc is None:
        ccs = []
    else:
        ccs = cc.split(",")

    if bcc is None:
        bccs = []
    else:
        bccs = bcc.split(",")

    if reply_to is None:
        replyTo = []
    else:
        replyTo = reply_to.split(",")

    destination = {
        'ToAddresses': [recipient_email],
        'CcAddresses': ccs,
        'BccAddresses': bccs,
    }

    body = {'Text': {'Charset': charset, 'Data': text}}

    if html is not None:
        body.update(Html={'Charset': charset, 'Data': html})

    message = {'Body': body, 'Subject': {'Charset': charset, 'Data': subject}}

    log.debug(f'SES message for recipient with email {recipient_email}', message)

    client = boto3.client('ses', region_name=REGION)

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination=destination,
            Message=message,
            Source=sender_email,
            ReplyToAddresses=replyTo,
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        log.error(f'Failed to send email {e}')
        return None
    else:
        log.debug("SES Email sent!")

    return response
