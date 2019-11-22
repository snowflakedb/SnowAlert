import asyncio
import aiohttp
import json
from json.decoder import JSONDecodeError
from random import random
from dateutil.parser import parse as parse_date

from connectors.utils import updated
from runners.helpers import db, log


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
REQUEST_SPREAD_IN_SECONDS = 180


async def fetch(session, url, fetch_over=0):
    if fetch_over:
        await asyncio.sleep(fetch_over * random())
    async with session.get(
        f'https://snowflake.jamfcloud.com/JSSResource{url}', headers=HEADERS
    ) as response:
        txt = await response.text()
        result = {'recorded_at': parse_date(response.headers.get('Date'))}
        try:
            return updated(result, json.loads(txt))
        except JSONDecodeError:
            log.info(f'GET {url} -> status({response.status}) text({txt})')
            return result


def fetch_computer(s, cid):
    return fetch(s, f'/computers/id/{cid}', fetch_over=REQUEST_SPREAD_IN_SECONDS)


async def main(table_name):
    async with aiohttp.ClientSession() as session:
        cids = [
            c['id'] for c in (await fetch(session, '/computers')).get('computers', [])
        ]

        log.info(f'loading {len(cids)} computer details')
        computers = await asyncio.gather(
            *[fetch_computer(session, cid) for cid in cids]
        )

        log.info(f'inserting {len(computers)} computers into {table_name}')
        db.insert(
            table_name,
            [
                updated(
                    c.get('computer'), computer_id=cid, recorded_at=c.get('recorded_at')
                )
                for cid, c in zip(cids, computers)
            ],
        )


def ingest(table_name, options):
    global HEADERS
    creds = options.get('credentials', '')
    HEADERS = {'Authorization': f'Basic {creds}', 'Accept': 'application/json'}

    asyncio.get_event_loop().run_until_complete(main(f'data.{table_name}'))
