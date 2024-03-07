import asyncio
import aiohttp
import json
from json.decoder import JSONDecodeError
from random import random
from dateutil.parser import parse as parse_date

from connectors.utils import updated
from runners.helpers import db, log
from runners.utils import groups_of
from urllib.parse import urlencode
import requests


CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'credentials',
        'title': "API Credentials",
        'prompt': "b64(username:password)",
        'placeholder': "bWVvdzpodW50cmVzczIK",
        'secret': True,
        'required': True,
    }
]

HEADERS: dict = {}
REQUEST_SPEED_PER_SECOND = 10


async def fetch(session, url, wait=0) -> dict:
    if wait:
        await asyncio.sleep(wait)
    async with session.get(
        f'https://snowflake.jamfcloud.com/JSSResource{url}', headers=HEADERS
    ) as response:
        txt = await response.text()
        date_header = response.headers.get('Date')
        if date_header is None:
            log.info(f'GET {url} -> status({response.status}) text({txt})')
            return {}

        result = {'recorded_at': parse_date(date_header)}
        try:
            return updated(result, json.loads(txt))
        except JSONDecodeError:
            log.info(f'GET {url} -> status({response.status}) text({txt})')
            return result


def fetch_computer(s, cid, i=0):
    return fetch(s, f'/computers/id/{cid}', wait=i / REQUEST_SPEED_PER_SECOND)


async def main(table_name):
    async with aiohttp.ClientSession() as session:
        cids = [
            c['id'] for c in (await fetch(session, '/computers')).get('computers', [])
        ]

        log.info(f'loading {len(cids)} computer details')
        computers = await asyncio.gather(
            *[fetch_computer(session, cid, i) for i, cid in enumerate(cids)]
        )

        log.info(f'inserting {len(computers)} computers into {table_name}')
        rows = [
            updated(
                c.get('computer'), computer_id=cid, recorded_at=c.get('recorded_at')
            )
            for cid, c in zip(cids, computers)
        ]
        for g in groups_of(100, rows):
            db.insert(table_name, g)
        return len(rows)


def ingest(table_name, options):
    global HEADERS
    token = getAccessToken(json.loads(options['credentials']))
    HEADERS = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
    return asyncio.get_event_loop().run_until_complete(main(f'data.{table_name}'))


def getAccessToken(credentials: dict) -> str:
    """
    Args:
      credentials (dict): for jamfcloud oauth API, e.g. the json type:
        {
          "grant_type": "client_credentials",
          "client_id": str,
          "client_secret": str
        }
    """
    response = requests.post(
        'https://snowflake.jamfcloud.com/api/oauth/token',
        data=credentials,
    )

    access_token = response.json().get('access_token')

    assert access_token is not None, "no access token in jamf oauth response"

    return access_token
