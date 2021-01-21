import os

from pymsteams import connectorcard, cardsection

from runners.helpers import log
from runners.helpers import vault


def handle(alert,
           type='msteams',
           webhook=None,
           title=None,
           color=None,
           message=None):
    """ Handler for the MS Teams integration utilizing the pymsteams library """

    if not webhook and not os.environ.get('MSTEAMS_WEBHOOK'):
        # log.info(f"No Webhook is provided nor there is a MSTEAMS_WEBHOOK in env, skipping handler.")
        return None

    webhook = webhook or vault.decrypt_if_encrypted(os.environ['MSTEAMS_WEBHOOK'])

    if message is None:
        log.error('Message is empty')
        return None

    # You must create the connectorcard object with the Microsoft Webhook URL
    m = connectorcard(webhook)
    
    if title:
        m.title(f'SnowAlert: {title}')
    else:
        m.title('SnowAlert')
    
    if color:
        # setting a hex color for the message
        m.color(color)
    
    # Add text to the message.
    if message:
        m.text(message)
        
    log.debug(
        'Microsoft Teams message for via webhook', message
    )
    
    # send the message.
    m.send()

    if m.last_http_status.status_code != 300:
        log.error(f"MS Teams handler error", m.last_http_status.text)
        return None

    return m.last_http_status
