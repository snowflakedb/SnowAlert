from codecs import encode
from json import dumps, loads
import re

from vault import decrypt_if_encrypted
from protocols import https, smtp, cloudwatch_metric


def zip(s, chunk_size=5_000_000):
    '''zip in pieces, as it is tough to inflate large chunks in Snowflake per UDF mem limits'''
    do_zip = lambda s: encode(encode(s.encode(), encoding='zlib'), 'base64').decode()
    if len(s) > chunk_size:
        return [do_zip(s[chunk_size:])] + zip(s[:chunk_size], chunk_size)
    return do_zip(s)


def format(s, ps):
    """format string s with params ps, preserving type of singular references

    >>> format('{0}', [{'a': 'b'}])
    {'a': 'b'}
    """
    m = re.match('{(\d+)}', s)
    return ps[int(m.group(1))] if m else s.format(*ps)


def lambda_handler(event, context=None):
    headers = event['headers']
    response_encoding = headers.pop('sf-custom-response-encoding', None)

    req_body = loads(event['body'])
    res_data = []
    for row_number, *args in req_body['data']:
        row_result = []
        header_params = {
            k.replace('sf-custom-', '').replace('-', '_'): format(v, args)
            for k, v in headers.items()
            if k.startswith('sf-custom-')
        }

        try:

            if event['path'] == '/https':
                row_result = https(**header_params)

            elif event['path'] == '/smtp':
                row_result = smtp(**header_params)

            elif event['path'] == '/cloudwatch_metric':
                row_result = cloudwatch_metric(**header_params)

        except Exception as e:
            row_result = {'error': repr(e)}

        res_data.append(
            [
                row_number,
                zip(dumps(row_result)) if response_encoding == 'gzip' else row_result,
            ]
        )

    return {'statusCode': 200, 'body': dumps({'data': res_data})}
