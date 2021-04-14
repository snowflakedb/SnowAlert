from codecs import encode
from importlib import import_module
from json import dumps, loads
import re
import sys
import os.path

from vault import decrypt_if_encrypted

# pip install --target ./site-packages -r requirements.txt
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(dir_path, 'site-packages'))


def zip(s, chunk_size=1_000_000):
    '''zip in pieces, as it is tough to inflate large chunks in Snowflake per UDF mem limits'''
    do_zip = lambda s: encode(encode(s.encode(), encoding='zlib'), 'base64').decode()
    if len(s) > chunk_size:
        return [do_zip(s[:chunk_size])] + zip(s[chunk_size:], chunk_size)
    return [do_zip(s)]


def format(s, ps):
    """format string s with params ps, preserving type of singular references

    >>> format('{0}', [{'a': 'b'}])
    {'a': 'b'}

    >>> format('{"z": [{0}]}', [{'a': 'b'}])

    """

    def replace_refs(s, ps):
        for i, p in enumerate(ps):
            old = '{' + str(i) + '}'
            new = dumps(p) if isinstance(p, (list, dict)) else str(p)
            s = s.replace(old, new)
        return s

    m = re.match('{(\d+)}', s)
    return ps[int(m.group(1))] if m else replace_refs(s, ps)


def lambda_handler(event, context=None):
    headers = event['headers']
    response_encoding = headers.pop('sf-custom-response-encoding', None)

    req_body = loads(event['body'])
    res_data = []
    for row_number, *args in req_body['data']:
        row_result = []
        processor_params = {
            k.replace('sf-custom-', '').replace('-', '_'): format(v, args)
            for k, v in headers.items()
            if k.startswith('sf-custom-')
        }

        try:

            protocol, *path = event['path'].lstrip('/').split('/')
            protocol = protocol.replace('-', '_')
            process_row = import_module(f'drivers.process_{protocol}').process_row
            row_result = process_row(*path, **processor_params)

        except Exception as e:
            row_result = {'error': repr(e)}

        res_data.append(
            [
                row_number,
                zip(dumps(row_result)) if response_encoding == 'gzip' else row_result,
            ]
        )

    data_dumps = dumps({'data': res_data})

    if len(data_dumps) > 6_000_000:
        data_dumps = dumps(
            {
                'data': [
                    [
                        rn,
                        {
                            'error': (
                                f'Response size ({len(data_dumps)} bytes) will likely'
                                'exceeded maximum allowed payload size (6291556 bytes).'
                            )
                        },
                    ]
                    for rn, *args in req_body['data']
                ]
            }
        )

    return {'statusCode': 200, 'body': data_dumps}
