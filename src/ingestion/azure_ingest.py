from os import environ
import yaml


from azure.storage.blob import BlockBlobService

from runners.helpers.auth import load_pkb_rsa
from runners.helpers.dbconfig import PRIVATE_KEY, PRIVATE_KEY_PASSWORD
from runners.helpers import db, log

from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile


def get_timestamp(table):

    timestamp_query = f"""
        SELECT EVENT_TIME from {table}
        order by EVENT_TIME desc
        limit 1
        """
    ts = list(db.fetch(timestamp_query))
    if len(ts) > 0:
        return ts[0]['EVENT_TIME']
    else:
        return None


def get_pipes():
    query = f"SHOW PIPES IN SNOWALERT.DATA"
    return db.fetch(query)


def main():

    for pipe in get_pipes():
        metadata = yaml.load(pipe['comment'])
        if metadata['type'] != 'Azure':
            log.info(f"{pipe['name']} is not an Azure pipe, and will be skipped.")
            break

        blob_name = metadata['blob']
        account_name = metadata['account']
        pipe_name = pipe['name']
        table = metadata['target']

        sas_token = environ.get('AZURE_SAS_TOKEN_'+metadata['suffix'])

        log.info(f"Now working on pipe {pipe_name}")

        block_blob_service = BlockBlobService(account_name=account_name, sas_token=sas_token)

        files = block_blob_service.list_blobs(blob_name)

        newest_time = get_timestamp(table)
        new_files = []
        if newest_time:
            for file in files:
                if file.properties.creation_time > newest_time:
                    new_files.append(StagedFile(file.name, None))
        else:
            for file in files:
                new_files.append(StagedFile(file.name, None))

        log.info(new_files)

        # now we have a list of filenames created after the most recent timestamp; we need to pass this to the snowpipe api

        # Proxy object that abstracts the Snowpipe REST API
        ingest_manager = SimpleIngestManager(account=environ.get('SNOWFLAKE_ACCOUNT'),
                                             host=f'{environ.get("SNOWFLAKE_ACCOUNT")}.snowflakecomputing.com',
                                             user=environ.get('SA_USER'),
                                             pipe=f'SNOWALERT.DATA.{pipe_name}',
                                             private_key=load_pkb_rsa(PRIVATE_KEY, PRIVATE_KEY_PASSWORD))
        if len(new_files) > 0:
            try:
                response = ingest_manager.ingest_files(new_files)
                log.info(response)
            except Exception as e:
                log.error(e)
                return


if __name__ == '__main__':
    main()
