from base64 import b64encode
from json import dumps, loads, JSONDecodeError
from re import match
from urllib.request import urlopen, Request, HTTPBasicAuthHandler
from urllib.parse import parse_qsl
from urllib.error import HTTPError

from vault import decrypt_if_encrypted
from utils import parse_header_links, pick


def parse_header_dict(value):
    return {k: decrypt_if_encrypted(v) for k, v in parse_qsl(value)}


def lambda_handler(event, context=None):
    headers = event['headers']

    # url, host, path
    if 'sf-custom-url' in headers:
        req_url = headers['sf-custom-url']
        m = match(r'^https://([^/]+)(.*)$', req_url)
        if m:
            req_host, request_path = m.groups()
        else:
            raise RuntimeError('url must start with https://')
    else:
        req_host = headers['sf-custom-host']
        req_path = headers.get('sf-custom-path', '/')
        req_url = f'https://{req_host}{req_path}'

    req_kwargs = parse_header_dict(headers.get('sf-custom-kwargs'))

    req_headers = {
        k: v.format(**req_kwargs)
        for k, v in parse_header_dict(headers.get('sf-custom-headers', '')).items()
    }
    if 'sf-custom-basicauth' in headers:
        usr, pwd = headers['sf-custom-basicauth'].split(':')
        auth = decrypt_if_encrypted(usr) + ':' + decrypt_if_encrypted(pwd)
        req_headers['Authorization'] = b'Basic ' + b64encode(auth.encode())

    # query, nextpage_path, results_path
    req_qs = headers.get('sf-custom-querystring', '')
    req_nextpage_path = headers.get('sf-custom-nextpage-path', '')
    req_results_path = headers.get('sf-custom-results-path', '')

    # load the data
    body = loads(event['body'])
    res_data = []
    for row, *args in body.get('data'):
        data = []

        if event['path'] == '/https':
            req_method = headers.get('sf-custom-method', 'get').format(*args).upper()
            if 'sf-custom-json' in headers:
                req_data = dumps(parse_header_dict(headers['sf-custom-json'].format(*args))).encode()
            else:
                req_data = headers.get('sf-custom-data', '').format(*args).encode()
            next_url = req_url.format(*args)
            next_url += ('?' + req_qs.format(*args)) if req_qs else ''

            while next_url:
                req = Request(
                    next_url, method=req_method, headers=req_headers, data=req_data
                )
                links_headers = None
                try:
                    response = urlopen(req)
                    links_headers = parse_header_links(','.join(response.headers.get_all('link', [])))
                    response_body = response.read()
                    result = pick(req_results_path, loads(response_body))
                except HTTPError as e:
                    result = {'error': f'{e.status} {e.reason}'}
                except JSONDecodeError as e:
                    result = {'error': 'JSONDecodeError', 'text': response_body.decode()}

                if req_nextpage_path and isinstance(result, list):
                    data += result
                    nextpage = pick(req_nextpage_path, result)
                    next_url = f'https://{req_host}{nextpage}' if nextpage else None
                elif links_headers and isinstance(result, list):
                    data += result
                    next_url = next((l for l in links_headers if l['rel'] == 'next'), {}).get('url')
                else:
                    data = result
                    next_url = None

        elif event['path'] == '/smtp':
            header_params = {
                k.replace('sf-custom-', ''): v.format(*args)
                for k, v in headers.items()
                if k.startswith('sf-custom-')
            }
            data = smtp(**header_params)

        res_data.append([row, data])

    return {'statusCode': 200, 'body': dumps({'data': res_data})}


import smtplib
import ssl

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def smtp(
    user,
    password,
    to,
    sender_email=None,
    text=None,
    html=None,
    subject=None,
    reply_to=None,
    cc=None,
    bcc=None,
    host='smtp.gmail.com',
    port=587,
    use_ssl=True,
    use_tls=True,
):

    user = decrypt_if_encrypted(user)
    password = decrypt_if_encrypted(password)
    sender_email = sender_email or user

    if to is None:
        log.error(f"param 'to' required")
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
    message['To'] = to

    recipients = to.split(',')

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
