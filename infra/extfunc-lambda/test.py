from json import loads
from os import environ
from urllib.parse import urlencode

from lambda_function import lambda_handler


def test_abuseipdb():
    key = environ.get('ABUSEIPDB_KEY')
    if key:
        result = lambda_handler({
            'path': '/https',
            'headers': {
                'sf-custom-headers': urlencode({'Key': key}),
                'sf-custom-querystring': 'ipAddress={0}',
                'sf-custom-url': 'https://api.abuseipdb.com/api/v2/check',
                'sf-custom-results-path': 'data',
            },
            'body': '{"data": [[0, "127.0.0.1"]]}',
        })

        assert {'isp': 'Loopback'}.items() < loads(result['body'])['data'][0][1].items()


def test_zengrc():
    zid = environ.get('ZENGRC_ID')
    zsc = environ.get('ZENGRC_SECRET')
    zho = environ.get('ZENGRC_HOST')

    if zid and zsc and zho:
        result = lambda_handler({
            'path': '/https',
            'headers': {
                'sf-custom-basicauth': f'{zid}:{zsc}',
                'sf-custom-host': zho,
                'sf-custom-path': '/api/v2/{0}',
                'sf-custom-nextpage-path': 'links.next.href',
                'sf-custom-results-path': 'data'
            },
            'body': '{"data": [[0, "assessments"]]}',
        })

        assert len(loads(result['body'])['data'][0][1]) > 50


def test_tenable_start():
    ts = environ.get('TENABLE_SECRET')
    tc = environ.get('TENABLE_CLIENT')

    if ts and tc:
        result = lambda_handler({
            'path': '/https',
            'headers': {
                'sf-custom-host': 'cloud.tenable.com',
                'sf-custom-path': '/vulns/export/status',
                'sf-custom-kwargs': urlencode({'apisecret': ts}),
                'sf-custom-headers': urlencode({'X-ApiKeys': f'accessKey={tc};secretKey={{apisecret}}'}),
                'sf-custom-results-path': 'exports',
            },
            'body': '{"data": [[0]]}',
        })

        assert len(loads(result['body'])['data'][0][1]) > 50


def test_slack():
    ts = environ.get('SLACK_TOKEN')

    if ts:
        result = lambda_handler({
            'path': '/https',
            'headers': {
                'sf-custom-host': 'slack.com',
                'sf-custom-method': '{0}',
                'sf-custom-path': '/api/{1}',
                'sf-custom-data': '{2}',
                'sf-custom-kwargs': urlencode({'token': ts}),
                'sf-custom-headers': urlencode({'Authorization': f'Bearer {{token}}'}),
            },
            'body': '{"data": [[0, "post", "conversations.replies", "channel=DRRBNRZ1V&ts=1598610858.000700"]]}',
        })

        assert len(loads(result['body'])['data'][0][1]['messages'][1]['text']) > 5



def test_email():
    tc = environ.get('EMAIL_CLIENT')
    ts = environ.get('EMAIL_PASSWORD')

    if ts and tc:
        result = lambda_handler({
            'path': '/smtp',
            'headers': {
                'sf-custom-host': 'smtp.gmail.com',
                'sf-custom-user': tc,
                'sf-custom-password': ts,
                'sf-custom-to': '{0}',
                'sf-custom-subject': '{1}',
                'sf-custom-text': '{2}'
            },
            'body': '{"data": [[0, "andrey.fedorov@snowflake.com", "test subject!", "testing 1 2 3"]]}',
        })

        assert loads(result['body'])['data'][0][1] == {}


if __name__ == '__main__':
    test_email()
