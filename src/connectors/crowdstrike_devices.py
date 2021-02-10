"""Crowdstrike Devices
Collect Crowdstrike Device information using a Client ID and Secret
"""

from typing import Coroutine, Generator, Dict, Tuple
from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE
from datetime import datetime
import aiohttp
import asyncio
from functools import reduce
import requests
from .utils import yaml_dump

PAGE_SIZE = 1000

CROWDSTRIKE_URL = 'https://api.crowdstrike.com'
CROWDSTRIKE_AUTH_TOKEN_URL = f'{CROWDSTRIKE_URL}/oauth2/token'
CROWDSTRIKE_DEVICES_BY_ID_URL = f'{CROWDSTRIKE_URL}/devices/queries/devices-scroll/v1'
CROWDSTRIKE_DEVICE_DETAILS_URL = f'{CROWDSTRIKE_URL}/devices/entities/devices/v1'
SPOTLIGHT_API_URL = f'{CROWDSTRIKE_URL}/spotlight'
VULNS_QUERY_VERSION = "v1"
CROWDSTRIKE_VULN_QUERY_URL = (
    f'{SPOTLIGHT_API_URL}/queries/vulnerabilities/{VULNS_QUERY_VERSION}'
)
VULNS_ENT_VERSION = "v2"
CROWDSTRIKE_VULN_ENTITY_URL = (
    f'{SPOTLIGHT_API_URL}/entities/vulnerabilities/{VULNS_ENT_VERSION}'
)
REMEDIATIONS_VERSION = "v2"
CROWDSTRIKE_REMEDIATIONS_URL = (
    f'{SPOTLIGHT_API_URL}/entities/remediations/{REMEDIATIONS_VERSION}'
)

CONNECTION_OPTIONS = [
    {
        'name': 'client_id',
        'title': "Crowdstrike API Client ID",
        'prompt': "Your Crowdstrike Client ID",
        'type': 'str',
        'required': True,
        'secret': True,
    },
    {
        'name': 'client_secret',
        'title': "Crowdstrike API Client Secret",
        'prompt': "Your Crowdstrike API Client Secret",
        'type': 'str',
        'secret': True,
        'required': True,
    },
]

LANDING_DEVICES_TABLE_COLUMNS = [
    ('INSERT_ID', 'NUMBER IDENTITY START 1 INCREMENT 1'),
    ('SNAPSHOT_AT', 'TIMESTAMP_LTZ(9)'),
    ('RAW', 'VARIANT'),
    ('DEVICE_ID', 'STRING'),
    ('FIRST_SEEN', 'TIMESTAMP_LTZ(9)'),
    ('SYSTEM_MANUFACTURER', 'STRING'),
    ('CONFIG_ID_BASE', 'NUMBER'),
    ('LAST_SEEN', 'TIMESTAMP_LTZ(9)'),
    ('POLICIES', 'VARIANT'),
    ('SLOW_CHANGING_MODIFIED_TIMESTAMP', 'TIMESTAMP_LTZ(9)'),
    ('MINOR_VERSION', 'NUMBER'),
    ('SYSTEM_PRODUCT_NAME', 'STRING'),
    ('HOSTNAME', 'STRING'),
    ('MAC_ADDRESS', 'STRING'),
    ('PRODUCT_TYPE_DESC', 'STRING'),
    ('PLATFORM_NAME', 'STRING'),
    ('EXTERNAL_IP', 'STRING'),
    ('AGENT_LOAD_FLAGS', 'NUMBER'),
    ('GROUP_HASH', 'STRING'),
    ('PROVISION_STATUS', 'STRING'),
    ('OS_VERSION', 'STRING'),
    ('GROUPS', 'VARIANT'),
    ('BIOS_VERSION', 'STRING'),
    ('MODIFIED_TIMESTAMP', 'TIMESTAMP_LTZ(9)'),
    ('LOCAL_IP', 'STRING'),
    ('AGENT_VERSION', 'STRING'),
    ('MAJOR_VERSION', 'NUMBER'),
    ('META', 'VARIANT'),
    ('AGENT_LOCAL_TIME', 'TIMESTAMP_LTZ(9)'),
    ('BIOS_MANUFACTURER', 'STRING'),
    ('PLATFORM_ID', 'NUMBER'),
    ('DEVICE_POLICIES', 'VARIANT'),
    ('CONFIG_ID_BUILD', 'NUMBER'),
    ('CONFIG_ID_PLATFORM', 'NUMBER'),
    ('CID', 'STRING'),
    ('STATUS', 'STRING'),
    ('SERVICE_PACK_MINOR', 'NUMBER'),
    ('PRODUCT_TYPE', 'NUMBER'),
    ('SERVICE_PACK_MAJOR', 'NUMBER'),
    ('BUILD_NUMBER', 'NUMBER'),
    ('POINTER_SIZE', 'NUMBER'),
    ('SITE_NAME', 'STRING'),
    ('MACHINE_DOMAIN', 'STRING'),
    ('OU', 'VARIANT'),
]

LANDING_SPOTLIGHT_VULNS_TABLE_COLUMNS = [
    ('raw', 'VARIANT'),
    ('event_time', 'TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP'),
]

REM_ID_COL = "REM_ID"
LANDING_SPOTLIGHT_REMS_TABLE_COLUMNS = [
    ('RAW', 'VARIANT'),
    (REM_ID_COL, 'STRING'),
    ('EVENT_TIME', 'TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP'),
]


def spotlight_rems_table_name(connection_name: str = "DEFAULT"):
    return f"data.CROWDSTRIKE_SPOTLIGHT_REMS_{connection_name}_CONNECTION"


# Perform the authorization call to create access token for subsequent API calls
def get_token_basic(client_id: str, client_secret: str) -> str:
    headers: dict = {"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    try:
        log.debug(f"Preparing POST: url={CROWDSTRIKE_AUTH_TOKEN_URL}")
        req = requests.post(
            CROWDSTRIKE_AUTH_TOKEN_URL,
            headers=headers,
            auth=requests.auth.HTTPBasicAuth(client_id, client_secret),
        )
        req.raise_for_status()
    except requests.HTTPError as http_err:
        log.error(f"Error GET: url={CROWDSTRIKE_AUTH_TOKEN_URL}")
        log.error(f"HTTP error occurred: {http_err}")
        raise http_err
    try:
        credential = req.json()
    except Exception as json_error:
        log.debug(f"JSON error occurred: {json_error}")
        log.debug(f"requests response {req}")
        raise (json_error)
    try:
        access_token = credential["access_token"]
    except BaseException:
        log.error("error auth request token")
        raise AttributeError("error auth request token")
    return access_token


# Parse out the offset value from the result.
def get_offset_from_devices_results(result: dict) -> str:
    if not isinstance(result, dict):
        log.error("the result is not a dict")
        raise TypeError("the result from Crowdstrike is not a dict")
    try:
        offset: str = result["meta"]["pagination"]["offset"]
    except BaseException:
        log.error("the offset is not present")
        log.error(result.get("meta", "No meta"))
        raise AttributeError("the offset is not present inside meta")
    return offset


# Perform a generic API call
async def get_data(token: str, url: str, params: dict = {}) -> dict:
    headers: dict = {"Authorization": f"Bearer {token}"}
    try:
        log.debug(f"Preparing GET: url={url} with params={params}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers) as response:
                return await response.json()
        # req = requests.get(url, params=params, headers=headers)
        # req.raise_for_status()
    except aiohttp.ClientError as http_err:
        log.error(f"Error GET: url={url}")
        log.error(f"HTTP error occurred: {http_err}")
        raise http_err
    # try:
    #     json = req.json()

    # except Exception as json_error:
    #     log.debug(f"JSON error occurred: {json_error}")
    #     log.debug(f"requests response {req}")
    #     json = {}
    # return json


# Helper function to format the /devices endpoint parameters
def create_url_params_get_devices(url: str, resources: list) -> str:
    params_get_devices: str = "?"
    for id in resources:
        params_get_devices += "&ids=" + id
    return url + params_get_devices


def connect(connection_name, options):
    devices_table_name = f'crowdstrike_devices_{connection_name}_connection'
    spotlight_vulns_table_name = (
        f'crowdstrike_spotlight_vulns_{connection_name}_connection'
    )
    landing_devices_table = f'data.{devices_table_name}'
    landing_spotlight_vulns_table = f'data.{spotlight_vulns_table_name}'
    landing_spotlight_rems_table = spotlight_rems_table_name(connection_name)

    comment = yaml_dump(module='crowdstrike_devices', **options)

    db.create_table(
        name=landing_devices_table, cols=LANDING_DEVICES_TABLE_COLUMNS, comment=comment
    )
    db.execute(f'GRANT INSERT, SELECT ON {landing_devices_table} TO ROLE {SA_ROLE}')

    db.create_table(
        name=landing_spotlight_vulns_table,
        cols=LANDING_SPOTLIGHT_VULNS_TABLE_COLUMNS,
        comment=comment,
    )
    db.execute(
        f'GRANT INSERT, SELECT ON {landing_spotlight_vulns_table} TO ROLE {SA_ROLE}'
    )

    db.create_table(
        name=landing_spotlight_rems_table,
        cols=LANDING_SPOTLIGHT_REMS_TABLE_COLUMNS,
        comment=comment,
    )
    db.execute(
        f'GRANT INSERT, SELECT ON {landing_spotlight_rems_table} TO ROLE {SA_ROLE}'
    )
    return {
        'newStage': 'finalized',
        'newMessage': "Crowdstrike Devices ingestion table created!",
    }


async def ingest_devices(landing_table: str) -> Generator[int, None, None]:
    timestamp = datetime.utcnow()
    offset = ""
    params_get_id_devices: dict = {"limit": PAGE_SIZE, "offset": offset}

    while 1:
        dict_id_devices: dict = await authenticated_get_data(
            CROWDSTRIKE_DEVICES_BY_ID_URL, params_get_id_devices
        )
        resources: list = dict_id_devices["resources"]
        params_get_id_devices["offset"] = get_offset_from_devices_results(
            dict_id_devices
        )

        if len(resources) == 0:
            break

        device_details_url_and_params: str = create_url_params_get_devices(
            CROWDSTRIKE_DEVICE_DETAILS_URL, resources
        )

        dict_devices: dict = await authenticated_get_data(device_details_url_and_params)
        devices = dict_devices["resources"]

        db.insert(
            landing_table,
            values=[
                (
                    timestamp,
                    device,
                    device.get('device_id'),
                    device.get('first_seen', None),
                    device.get('system_manufacturer', None),
                    device.get('config_id_base', None),
                    device.get('last_seen', None),
                    device.get('policies', None),
                    device.get('slow_changing_modified_timestamp', None),
                    device.get('minor_version', None),
                    device.get('system_product_name', None),
                    device.get('hostname', None),
                    device.get('mac_address', None),
                    device.get('product_type_desc', None),
                    device.get('platform_name', None),
                    device.get('external_ip', None),
                    device.get('agent_load_flags', None),
                    device.get('group_hash', None),
                    device.get('provision_status', None),
                    device.get('os_version', None),
                    device.get('groups', None),
                    device.get('bios_version', None),
                    device.get('modified_timestamp', None),
                    device.get('local_ip', None),
                    device.get('agent_version', None),
                    device.get('major_version', None),
                    device.get('meta', None),
                    device.get('agent_local_time', None),
                    device.get('bios_manufacturer', None),
                    device.get('platform_id', None),
                    device.get('device_policies', None),
                    device.get('config_id_build', None),
                    device.get('config_id_platform', None),
                    device.get('cid', None),
                    device.get('status', None),
                    device.get('service_pack_minor', None),
                    device.get('product_type', None),
                    device.get('service_pack_major', None),
                    device.get('build_number', None),
                    device.get('pointer_size', None),
                    device.get('site_name', None),
                    device.get('machine_domain', None),
                    device.get('ou', None),
                )
                for device in devices
            ],
            select=db.derive_insert_select(LANDING_DEVICES_TABLE_COLUMNS),
            columns=db.derive_insert_columns(LANDING_DEVICES_TABLE_COLUMNS),
        )
        log.info(f'Inserted {len(devices)} rows.')
        yield len(devices)


rem_id_cache = {}


async def do_rem_ids_exist(rem_ids: list) -> list:
    """
    does_rem_exist returns "" if the remediation has already been cached locally
    or is recorded in the database. Otherwise, it returns the remediation ID.
    """

    def does_rem_exist(rem_id: str):
        # Check local cache if you have seen this ID in this run
        if rem_id in rem_id_cache:
            log.info(
                f"[CACHE]: Local cache hit! Already recorded {rem_id} in this round"
            )
            return ""

        # Check if we have it stored in the table already
        row = db.fetch_latest(
            spotlight_rems_table_name(),
            where=f"{REM_ID_COL}='{rem_id}'",
        )
        if row is not None:
            log.info(
                f"[CACHE]: Database cache hit! Already recorded {rem_id} in Snowflake"
            )
            rem_id_cache[rem_id] = True
            return ""

        return rem_id

    return filter(lambda x: x is not "", [does_rem_exist(rem_id) for rem_id in rem_ids])


async def get_uncached_rem_dets(vuln_dets: list) -> Coroutine[None, None, list]:
    """
    get_uncached_rem_dets returns the remediation details of only remediations that have not been recorded yet.
    According to the documentation, it pretty common for many of the vulns to share the same remediations.
    """
    # dedupe all the remediation IDs from all the vuln detail information.
    rem_ids = set(
        reduce(
            (lambda x, y: x + y if (y != None) else x),
            [det.get("remediation", {}).get("ids", []) for det in vuln_dets],
            [],
        )
    )
    # find the IDs that we have not already recorded
    uncached_rem_ids = await do_rem_ids_exist(rem_ids)
    if len(uncached_rem_ids) == 0:
        return []

    # update the cache
    for rem_id in uncached_rem_ids:
        rem_id_cache[rem_id] = True

    # pull the data
    resp = await authenticated_get_data(
        CROWDSTRIKE_REMEDIATIONS_URL,
        [("ids", r) for r in uncached_rem_ids],
    )
    return resp.get("resources", [])


async def insert_rem_dets(dets: list) -> Coroutine[None, None, int]:
    """
    insert_rem_dets inserts a remediation into the database.
    """
    if len(dets) == 0:
        return 0

    numRows = db.insert(
        f'data.CROWDSTRIKE_SPOTLIGHT_REMS_DEFAULT_CONNECTION',
        values=[
            {'raw': det, 'rem_id': det['id'], 'event_time': datetime.utcnow()}
            for det in dets
        ],
        select=db.derive_insert_select(LANDING_SPOTLIGHT_REMS_TABLE_COLUMNS),
        columns=db.derive_insert_columns(LANDING_SPOTLIGHT_REMS_TABLE_COLUMNS),
    )["number of rows inserted"]

    log.info(f"[UPLOAD]: Inserted {numRows} rows in rem details")
    return numRows


async def get_vuln_dets(vuln_ids: list) -> Coroutine[None, None, list]:
    """
    get_vuln_dets pulls the vulnerability details information for a set of vuln IDs.
    """
    resp = await authenticated_get_data(CROWDSTRIKE_VULN_ENTITY_URL, vuln_ids)
    return resp.get("resources", [])


async def insert_vuln_dets(table_name: str, dets: list) -> Coroutine[None, None, int]:
    """
    insert_vuln_dets inserts the vuln details into the database.
    """
    if len(dets) == 0:
        return 0
    numRows = db.insert(
        table_name,
        values=[{'raw': det, 'event_time': datetime.utcnow()} for det in dets],
        select=db.derive_insert_select(LANDING_SPOTLIGHT_VULNS_TABLE_COLUMNS),
        columns=db.derive_insert_columns(LANDING_SPOTLIGHT_VULNS_TABLE_COLUMNS),
    )["number of rows inserted"]

    log.info(f"[UPLOAD]: Inserted {numRows} rows in vuln details")
    return numRows


async def get_and_insert_rem_dets(vuln_dets: list) -> Coroutine[None, None, int]:
    """
    get_and_insert_rem_dets dedupes and remediation IDs and then fetches their
    details from the API. The results are then inserted into the database.
    """
    dets = await get_uncached_rem_dets(vuln_dets)
    return await insert_rem_dets(dets)


async def pull_spotlight_vuln_details(
    table_name: str, vuln_ids: Tuple[str, str]
) -> Coroutine[None, None, list]:
    """
    pull_spotlight_vuln_details gets all the vuln detail information, inserts it
    into the database and retrieves any remediation IDs and pulls those into Snowflake
    as well.
    """
    dets: list = await get_vuln_dets(vuln_ids)
    return await asyncio.gather(
        insert_vuln_dets(table_name, dets),
        get_and_insert_rem_dets(dets),
    )


def authenticated_get_data_builder(options: Dict[str, str]):
    token = get_token_basic(
        options.get('client_id', ""), options.get('client_secret', "")
    )

    async def request(url: str, params: Dict[str, str] = {}):
        nonlocal token
        while True:
            try:
                return await get_data(token, url, params)
            except aiohttp.ClientError as http_err:
                log.info(http_err)
                if len(http_err.args) > 0 and "401" in http_err.args[0]:
                    log.info("Token expired, getting new token...")
                    token = get_token_basic(
                        options.get('client_id', ""), options.get('client_secret', "")
                    )
                    log.info(f"Retying request: {url} {params}")

    return request


async def authenticated_get_data(url: str, params: Dict[str, str] = {}):
    log.info("NOT IMPLEMENTED")


async def paginate_api(
    params: Dict[str, str],
    url: str,
    max_page_entries: int = 400,
):
    """
    paginate_api returns a generator that yields page entries from the specified
    Crowdstrike API. By default, page sizes are only 400 entries long because that maps
    to the maximim number of vuln IDs that can be specified in the vuln details API, but
    this is configurable.
    """
    page_entries = []
    page = {}
    after = "go"
    i = 0
    # This API provides an 'after' pagination key to indicate their are more pages.
    # If the key is empty, we know we are on the last page.
    while len(after) != 0:
        resp = await authenticated_get_data(url, params)
        page = resp.get("meta", {}).get("pagination", {})
        # Set the next pagination key.
        after = page.get("after", "")
        params["after"] = after
        # Gather page entries until we hit the max number of page entries to yield
        page_entries = resp.get("resources", []) + page_entries

        # Yield after we hit the max number of page entries
        if len(page_entries) >= max_page_entries:
            ret = page_entries[:max_page_entries]
            i += len(ret)
            page_entries = page_entries[max_page_entries:]
            yield ret, i, page.get("total", 0)

    yield page_entries, i + len(page_entries), page.get("total", 0)


async def pull_spotlight_data(table_name: str, params: Dict[str, str]) -> int:
    """
    pull_spotlight_data pulls all vulnerability and associated remediation data
    from the spotlight API based on the provided filter params. Filter params
    can be configured using the Falcon Query Language (FQL).
    """

    tasks = []
    async for vuln_ids, accum, total in paginate_api(
        params, CROWDSTRIKE_VULN_QUERY_URL
    ):
        if len(vuln_ids) == 0 | total == 0:
            break

        log.info(
            f"[DOWNLOAD]: {accum}/{total} ({'{:.0%}'.format(accum / total)}) vulns downloaded"
        )
        vid_tup = [("ids", v) for v in vuln_ids]
        task = asyncio.create_task(pull_spotlight_vuln_details(table_name, vid_tup))
        tasks.append(task)

    await asyncio.gather(*tasks)
    return accum


async def ingest_spotlight(table_name: str) -> int:
    """
    ingest_spotlight pulls the latest vulnerability data from the spotlight API
    NOTES:
      * When listing the vuln IDs, the API only returns at most 100 at a time, so this can take a bit
      * When listing the vuln details, the API only can retrieve at most 400 vuln IDs at a time
      * If a baseline of vuln data has not already been uploaded to Snowflake, this may take a while and several retrys based on how long credentials last for
      * This function automatically starts from the latest entry in Snowflake defaulting to 1999 if no entries are found
    """

    # Since this API has a lot of data, only get vuln data starting from the
    # last insertion date. This assumes someone has already done a bulk download
    # at the beginning to create a vuln baseline.
    row = db.fetch_latest(table_name, 'raw:created_timestamp')
    ts = datetime.strptime("1999-01-01", "%Y-%m-%d")
    if row is not None:
        ts = datetime.fromisoformat(row[: len(row) - 1])

    date = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    params: Dict[str, str] = {
        "filter": f"created_timestamp:>'{date}'",
        "sort": "created_timestamp|asc",
    }

    # Fill the local cache with the remediation IDs we already know about.
    for rem_id in db.fetch(f"select {REM_ID_COL} from {spotlight_rems_table_name()};"):
        rem_id_cache[rem_id[REM_ID_COL]] = True

    # pull the data
    return await pull_spotlight_data(table_name, params)


async def main(table_name: str):
    if table_name.startswith("data.CROWDSTRIKE_DEVICES"):
        return await ingest_devices(table_name)
    elif table_name.startswith("data.CROWDSTRIKE_SPOTLIGHT_VULNS"):
        return await ingest_spotlight(table_name)


def ingest(table_name: str, options: Dict[str, str]) -> Generator[int, None, None]:
    global authenticated_get_data
    authenticated_get_data = authenticated_get_data_builder(options)
    return asyncio.run(main(f'data.{table_name}'))
