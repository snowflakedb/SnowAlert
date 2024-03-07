from os import environ as env
import smtplib
import ssl

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from runners.helpers import log
from runners.helpers import vault


HOST = env.get('SA_SMTP_HOST', env.get('SMTP_SERVER', ''))
PORT = int(env.get('SA_SMTP_PORT', env.get('SMTP_PORT', 587)))
USER = env.get('SA_SMTP_USER', env.get('SMTP_USER', ''))
PASSWORD = env.get('SA_SMTP_PASSWORD', env.get('SMTP_PASSWORD', ''))
USE_SSL = env.get('SA_SMTP_USE_SSL', env.get('SMTP_USE_SSL', True))
USE_TLS = env.get('SA_SMTP_USE_TLS', env.get('SMTP_USE_TLS', True))

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
    host=HOST,
    port=PORT,
    user=USER,
    password=PASSWORD,
    use_ssl=USE_SSL,
    use_tls=USE_TLS,
):

    user = vault.decrypt_if_encrypted(user)
    password = vault.decrypt_if_encrypted(password)
    sender_email = sender_email or user

    if recipient_email is None:
        log.error(f"param 'recipient_email' required")
        return None

    if text is None:
        log.error(f"param 'text' required")
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

    if use_ssl is True:
        context = ssl.create_default_context()
        if use_tls is True:
            smtpserver = smtplib.SMTP(host, port)
            smtpserver.starttls(context=context)
        else:
            smtpserver = smtplib.SMTP_SSL(host, port, context=context)
    else:
        smtpserver = smtplib.SMTP(host, port)

    if user and password:
        smtpserver.login(user, password)

    result = smtpserver.sendmail(sender_email, recipients, message.as_string())
    smtpserver.close()

    return result
