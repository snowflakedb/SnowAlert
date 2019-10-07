import os
import smtplib
import ssl

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from runners.helpers import log
from runners.helpers import vault


def handle(
    alert,
    type='smtp',
    sender_email=None,
    recipient_email=None,
    text=None,
    html=None,
    subject=None,
    reply_to=None,
    cc=None,
    bcc=None,
):

    if not os.environ.get('SMTP_SERVER'):
        log.info("No SMTP_SERVER in env, skipping handler.")
        return None

    smtp_server = os.environ['SMTP_SERVER']

    if 'SMTP_PORT' in os.environ:
        smtp_port = os.environ['SMTP_PORT']
    else:
        smtp_port = 587

    if 'SMTP_USE_SSL' in os.environ:
        smtp_use_ssl = os.environ['SMTP_USE_SSL']
    else:
        smtp_use_ssl = True

    if 'SMTP_USE_TLS' in os.environ:
        smtp_use_tls = os.environ['SMTP_USE_TLS']
    else:
        smtp_use_tls = True

    smtp_user = vault.decrypt_if_encrypted(os.environ['SMTP_USER'])
    smtp_password = vault.decrypt_if_encrypted(os.environ['SMTP_PASSWORD'])

    if recipient_email is None:
        log.error(f"Cannot identify recipient email")
        return None

    if text is None:
        log.error(f"SES Message is empty")
        return None

    # Create the base MIME message.
    if html is None:
        message = MIMEMultipart()
    else:
        message = MIMEMultipart('alternative')

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first

    # Turn these into plain/html MIMEText objects
    textPart = MIMEText(text, 'plain')
    message.attach(textPart)

    if html is not None:
        htmlPart = MIMEText(html, 'html')
        message.attach(htmlPart)

    message['Subject'] = subject
    message['From'] = sender_email
    message['To'] = recipient_email

    recipients = recipient_email.split(',')

    if cc is not None:
        message['Cc'] = cc
        recipients = recipients + cc.split(',')

    if bcc is not None:
        recipients = recipients + bcc.split(',')

    if reply_to is not None:
        message.add_header('reply-to', reply_to)

    if smtp_use_ssl is True:
        context = ssl.create_default_context()
        if smtp_use_tls is True:
            smtpserver = smtplib.SMTP(smtp_server, smtp_port)
            smtpserver.starttls(context=context)
        else:
            smtpserver = smtplib.SMTP_SSL(smtp_server, smtp_port, context=context)
    else:
        smtpserver = smtplib.SMTP(smtp_server, smtp_port)

    smtpserver.login(smtp_user, smtp_password)
    smtpserver.sendmail(sender_email, recipients, message.as_string())
    smtpserver.close()
