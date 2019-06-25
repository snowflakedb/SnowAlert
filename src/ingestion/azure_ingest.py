from os import environ
import yaml


from azure.storage.blob import BlockBlobService

from runners.helpers.auth import load_pkb_rsa
from runners.helpers.dbconfig import PRIVATE_KEY, PRIVATE_KEY_PASSWORD
from runners.helpers import db, log, vault

from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile


def get_timestamp(table):

    timestamp_query = f"""
        SELECT loaded_on
        FROM {table}
        ORDER BY loaded_on DESC
        LIMIT 1
        """
    ts = next(db.fetch(timestamp_query), None)
    return ts['LOADED_ON'] if ts else None


def main():
    for pipe in db.get_pipes('data'):
        metadata = yaml.load(pipe['comment'])
        if metadata and metadata.get('type') != 'Azure':
            log.info(f"{pipe['name']} is not an Azure pipe, and will be skipped.")
            continue

        blob_name = metadata['blob']
        account_name = metadata['account']
        pipe_name = pipe['name']
        table = metadata['target']

        sas_token_envar = 'AZURE_SAS_TOKEN_' + metadata.get('suffix', '')
        if sas_token_envar in environ:
            encrypted_sas_token = environ.get(sas_token_envar)
        elif 'encrypted_sas_token' in metadata:
            encrypted_sas_token = metadata['encrypted_sas_token']
        else:
            log.info(f"{pipe['name']} has no azure auth")
            continue

        sas_token = vault.decrypt_if_encrypted(encrypted_sas_token)

        log.info(f"Now working on pipe {pipe_name}")

        endpoint_suffix = metadata.get('endpoint_suffix', 'core.windows.net')

        block_blob_service = BlockBlobService(
            account_name=account_name,
            sas_token=sas_token,
            endpoint_suffix=endpoint_suffix
        )

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
