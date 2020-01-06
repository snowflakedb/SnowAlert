"""Salesforce Event Log
Retrieve hourly Salesforce event log files from the API
"""

from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE
from .utils import yaml_dump
from simple_salesforce import Salesforce

import datetime,requests,csv,io,json,shutil,os,tempfile

CONNECTION_OPTIONS = [
    {
        'name': 'username',
        'title': "Salesforce Username",
        'prompt': "The username for API authentication",
        'type': 'str',
        'placeholder': "me@mydomain.com",
        'required': True
    },
    {
        'name': 'password',
        'title': "Salesforce Password",
        'prompt': "The password for API authentication",
        'type': 'str',
        'secret': True,
        'required': True
    },
    {
        'name': 'security_token',
        'title': "Salesforce Security Token",
        'prompt': "The Security Token for API authentication, associated with the user",
        'type': 'str',
        'secret': True,
        'required': True
    },
    {
        'name': 'environment',
        'title': "Salesforce Environment",
        'prompt': "Choose between Test (Sandbox) environment and Production environment",
        'type': 'select',
        'options': [
            { 'value': 'prod', 'label': "Production" },
            { 'value': 'test', 'label': "Test" }
        ],
        'type': 'str',
        'secret': True,
        'required': True
    }
]

LANDING_TABLE_COLUMNS = [('raw', 'VARIANT')]

def connect(connection_name, options):
    table_name = f'salesforce_events_{connection_name}'
    landing_log_table = f'data.{table_name}_connection'

    comment = yaml_dump(module='salesforce_event_log', **options)

    db.create_table(
        name=landing_log_table, cols=LANDING_TABLE_COLUMNS, comment=comment, 
        stage_file_format='TYPE = JSON STRIP_OUTER_ARRAY = TRUE', stage_copy_options='PURGE = TRUE'
    )
    db.execute(f'GRANT INSERT, SELECT ON {landing_log_table} TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': "Salesforce Event Log ingestion table created!",
    }

def ingest(table_name, options):
    landing_table = f'data.{table_name}'
    username = options['username']
    password = options['password']
    security_token = options['security_token']
    environment_raw = options['environment']
    environment = 'test' if environment_raw=='test' else None
    
    # We will fetch EventLogFiles where the LogDate is greater than the maximum timestamp seen in all previous EventLogFiles
    start_time = db.fetch_latest(
        landing_table,
        col='raw:TIMESTAMP_DERIVED'
    )
    if start_time is None: start_time = '1900-01-01T00:00:00.000Z'

    # TODO: Support more auth methods, including client certificates.
    sf = Salesforce(username=username, password=password, 
                    security_token=security_token, client_id='SnowAlert', domain=environment)
    event_log_soql_query = f'SELECT Id, EventType, LogDate FROM EventLogFile WHERE Interval=\'Hourly\' and LogDate > {start_time}'
    log.info(f'Querying event logs: {event_log_soql_query}')
    log_files = sf.query_all(event_log_soql_query)

    # Create a temp directory only accessible by the current user, which we will delete after Snowflake upload
    temp_dir = tempfile.mkdtemp('_sfevents')

    # Salesforce will provide a bunch of files, an hourly extract of each of the different event types in CSV format
    # There are around 50 different event types and they all have different fields. Rather than a table per event type,
    # we'll convert them to JSON and do schema-on-read. 
    # We'll load from the table stage which has the 'STRIP_OUTER_ARRAY' option, so there will be one row per event.
    total_files = log_files['totalSize']
    log.info(f'Found {total_files} event files to load.')
    if total_files > 0:
        for record in log_files['records']:
            url = record['attributes']['url']
            id = record['Id']
            log.info(f'Downloading event log file {id} from {url}.')
            # The URL provided is relative, but includes part of the base URL which we have to trim out before combining
            # E.g. it could look like /services/data/v38.0/sobjects/EventLogFile/0AT0o00000NSIv5GAB
            # where the base URL will look like: https://ap8.salesforce.com/services/data/v38.0/
            url_relative = 'sobjects/'+url.split('sobjects/')[1]+'/LogFile'
            result = sf._call_salesforce('GET',sf.base_url+url_relative,name=url_relative)
            # TODO: Investigate streaming the result and converting to JSON in chunks. 
            # Current method has high memory requirements for large files, but unlikely to be 
            # multi-GB hourly unless it's a really busy Salesforce org.
            reader = csv.DictReader(io.StringIO(result.text))        
            file_path = os.path.join(temp_dir,id+'.json')
            with open(file_path,'w') as f:
                # This will create a single line JSON file containing an array of objects
                json.dump(list(reader),f)
        # Copy all the staged .json files into the landing table
        log.info(f'Uploading all files to Snowflake stage: {table_name}.')
        db.copy_file_to_table_stage(table_name, os.path.join(temp_dir,'*.json'))
        log.info(f'Upload successful, deleting all local files.')
        shutil.rmtree(temp_dir)
        # The table is configured to purge upon load from its stage, so we don't need to clean up
        log.info(f'Copying events into Snowflake table from staged files.')
        db.load_from_table_stage(table_name)
        log.info(f'Loaded {total_files} event files.')
    else:
        log.info(f'Skipping load as there are no new event files.')
    return total_files
