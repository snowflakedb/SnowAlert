from base64 import b64encode
from json import dumps, loads, JSONDecodeError
from re import match
from urllib.request import urlopen, Request, HTTPBasicAuthHandler
from urllib.parse import parse_qsl
from urllib.error import HTTPError, URLError

from vault import decrypt_if_encrypted
from utils import parse_header_links, pick


def parse_header_dict(value):
    return {k: decrypt_if_encrypted(v) for k, v in parse_qsl(value)}


def make_basic_header(auth):
    return b'Basic ' + b64encode(auth.encode())


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
        req_host = headers.get('sf-custom-host')
        req_path = headers.get('sf-custom-path', '/')

    req_kwargs = parse_header_dict(headers.get('sf-custom-kwargs'))

    req_headers = {
        k: v.format(**req_kwargs)
        for k, v in parse_header_dict(headers.get('sf-custom-headers', '')).items()
    }
    if 'sf-custom-auth' in headers:
        auth = decrypt_if_encrypted(headers['sf-custom-auth'])
        auth = parse_header_dict(auth) if auth else {}

        if 'host' in auth and not req_host:
            req_host = auth['host']

        if auth.get('host') != req_host:
            pass  # if host in ct, only send creds to that host
        elif 'basic' in auth:
            req_headers['Authorization'] = make_basic_header(auth['basic'])

    elif 'sf-custom-basicauth' in headers:
        basicauth = decrypt_if_encrypted(headers['sf-custom-basicauth'])
        req_headers['Authorization'] = make_basic_header(basicauth)

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
                req_data = dumps(
                    parse_header_dict(headers['sf-custom-json'].format(*args))
                ).encode()
            else:
                req_data = headers.get('sf-custom-data', '').format(*args).encode()

            req_url = f'https://{req_host}{req_path}'
            next_url = req_url.format(*args)
            next_url += ('?' + req_qs.format(*args)) if req_qs else ''

            while next_url:
                req = Request(
                    next_url, method=req_method, headers=req_headers, data=req_data
                )
                links_headers = None
                try:
                    res = urlopen(req)
                    links_headers = parse_header_links(
                        ','.join(res.headers.get_all('link', []))
                    )
                    response_body = res.read()
                    response = loads(response_body)
                    result = pick(req_results_path, response)
                except HTTPError as e:
                    result = {'error': f'{e.status} {e.reason}'}
                except URLError as e:
                    result = {
                        'error': f'URLError',
                        'reason': str(e.reason),
                        'host': req_host,
                    }
                except JSONDecodeError as e:
                    result = {
                        'error': 'JSONDecodeError',
                        'text': response_body.decode(),
                    }

                if req_nextpage_path and isinstance(result, list):
                    data += result
                    nextpage = pick(req_nextpage_path, response)
                    next_url = f'https://{req_host}{nextpage}' if nextpage else None
                elif links_headers and isinstance(result, list):
                    data += result
                    nu = next((l for l in links_headers if l['rel'] == 'next'), {}).get(
                        'url'
                    )
                    next_url = nu if nu != next_url else None
                else:
                    data = result
                    next_url = None

        elif event['path'] == '/smtp':
            header_params = {
                k.replace('sf-custom-', '').replace('-', '_'): v.format(*args)
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
    recipient_email,
    text,
    sender_email=None,
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

    try:
        result = smtpserver.sendmail(sender_email, recipients, message.as_string())
    except smtplib.SMTPDataError as e:
        result = {
            'error': 'SMTPDataError',
            'smtp_code': e.smtp_code,
            'smtp_error': e.smtp_error.decode(),
        }

    finally:
        smtpserver.close()

    return result
