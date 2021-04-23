from base64 import b64encode
from email.utils import parsedate_to_datetime
from json import JSONDecodeError, dumps, loads
from re import match
from typing import Dict, Union, Optional, List, Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl
from urllib.request import Request, urlopen

from utils import parse_header_links, pick
from vault import decrypt_if_encrypted


def make_basic_header(auth):
    return b'Basic ' + b64encode(auth.encode())


def parse_header_dict(value):
    return {k: decrypt_if_encrypted(v) for k, v in parse_qsl(value)}


def process_row(
    data: Optional[str] = None,
    base_url: str = '',
    url: str = '',
    json: Optional[str] = None,
    method: str = 'get',
    headers: str = '',
    kwargs: Union[Dict, str] = '',
    auth: str = None,
    params: str = '',
    verbose: bool = False,
    cursor: str = '',
    results_path: str = '',
):
    if url:
        req_url = base_url + url
        m = match(r'^https://([^/]+)(.*)$', req_url)
        if m:
            req_host, req_path = m.groups()
        else:
            raise RuntimeError("url must start with https://")
    else:
        req_host = base_url
        req_path = url or '/'

    req_kwargs = parse_header_dict(kwargs)

    req_headers = {
        k: v.format(**req_kwargs) for k, v in parse_header_dict(headers).items()
    }
    req_headers.setdefault('User-Agent', 'Snowflake SnowAlert External Function 1.0')
    if auth is not None:
        auth = decrypt_if_encrypted(auth)
        assert auth is not None
        req_auth = (
            loads(auth)
            if auth.startswith('{')
            else parse_header_dict(auth)
            if auth
            else {}
        )

        if 'host' in req_auth and not req_host:
            req_host = req_auth['host']

        if req_auth.get('host') != req_host:
            pass  # if host in ct, only send creds to that host
        elif 'basic' in req_auth:
            req_headers['Authorization'] = make_basic_header(req_auth['basic'])
        elif 'bearer' in req_auth:
            req_headers['Authorization'] = f"Bearer {req_auth['bearer']}"
        elif 'authorization' in req_auth:
            req_headers['authorization'] = req_auth['authorization']

    # query, nextpage_path, results_path
    req_params: str = params
    req_results_path: str = results_path
    req_cursor: str = cursor
    req_method: str = method.upper()

    if json:
        req_data: Optional[bytes] = (
            json if json.startswith('{') else dumps(parse_header_dict(json))
        ).encode()
        req_headers['Content-Type'] = 'application/json'
    else:
        req_data = None if data is None else data.encode()

    req_url += f'?{req_params}'
    next_url: Optional[str] = req_url
    row_data: List[Any] = []

    while next_url:
        req = Request(next_url, method=req_method, headers=req_headers, data=req_data)
        links_headers = None
        try:
            res = urlopen(req)
            links_headers = parse_header_links(
                ','.join(res.headers.get_all('link', []))
            )
            response_body = res.read()
            response_headers = dict(res.getheaders())
            response_date = (
                parsedate_to_datetime(response_headers['Date']).isoformat()
                if 'Date' in response_headers
                else None
            )
            response = (
                {
                    'body': loads(response_body),
                    'headers': response_headers,
                    'responded_at': response_date,
                }
                if verbose
                else loads(response_body)
            )
            result = pick(req_results_path, response)
        except HTTPError as e:
            result = {
                'error': f'{e.code} {e.reason}',
                'url': next_url,
            }
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

        if req_cursor and isinstance(result, list):
            row_data += result
            if ':' in req_cursor:
                cursor_path, cursor_param = req_cursor.rsplit(':', 1)
            else:
                cursor_path = req_cursor
                cursor_param = cursor_path.split('.')[-1]
            cursor_value = pick(cursor_path, response)
            next_url = (
                f'{req_url}&{cursor_param}={cursor_value}' if cursor_value else None
            )
        elif links_headers and isinstance(result, list):
            row_data += result
            link_dict: Dict[Any, Any] = next((l for l in links_headers if l['rel'] == 'next'), {})
            nu: Optional[str] = link_dict.get('url')
            next_url = nu if nu != next_url else None
        else:
            row_data = result
            next_url = None

    return row_data
