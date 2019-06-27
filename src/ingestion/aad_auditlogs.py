import time
import requests
import json
from datetime import datetime, timedelta

from adal import AuthenticationContext
from azure.storage.blob import BlockBlobService
from azure.storage.blob import ContentSettings
from azure.storage.common import TokenCredential

from runners.helpers import vault

# Azure Gov Cloud Endpoints
AAD_ENDPOINT_URI = "https://login.microsoftonline.us/"
GRAPH_ENDPOINT_URI = "https://graph.microsoft.us/"
STORAGE_ENDPOINT_SUFFIX = "core.usgovcloudapi.net"


def log(msg):
    print(str(datetime.now()) + " " + msg)


def save_aad_auditlogs(auditlog_type, tenant_id, client_id, client_secret,
                       storage_account, storage_container):
    METADATA_LAST_DATETIME = "last_datetime"
    METADATA_LAST_EXECUTION = "last_execution"

    log("Save " + auditlog_type + " to " + storage_account + "/" + storage_container)

    # Create AAD authentication context to use for obtaining access tokens
    auth_context = AuthenticationContext(AAD_ENDPOINT_URI + tenant_id)

    # Get access token for storage.azure.com
    storage_token_response = auth_context.acquire_token_with_client_credentials(
        "https://storage.azure.com/",
        client_id,
        client_secret
    )

    # Create Azure Blob service client
    blob_service = BlockBlobService(
        storage_account,
        endpoint_suffix=STORAGE_ENDPOINT_SUFFIX,
        token_credential=TokenCredential(storage_token_response['accessToken'])
    )

    # Create container if it does not yet exist
    blob_service.create_container(storage_container, fail_on_exist=False)

    # Get datetime of last record from container metadata
    # NOTE: Date strings have nanosecond precision so would require numpy.datetime64 for parsing
    container_metadata = blob_service.get_container_metadata(storage_container)
    last_datetime = ""
    if METADATA_LAST_DATETIME in container_metadata:
        last_datetime = container_metadata[METADATA_LAST_DATETIME]
    else:
        last_datetime = datetime.strftime(datetime.now() - timedelta(days=90), "%Y-%m-%dT%H:%M:%S.%fZ")

    log("Previous value container last_datetime=" + last_datetime + "")

    # Get access token for graph.microsoft.com
    graph_token_response = auth_context.acquire_token_with_client_credentials(
        GRAPH_ENDPOINT_URI, client_id, client_secret)

    # Initial request filtered by latest date time with a batch of 500
    if auditlog_type == "directoryAudits":
        datetime_record_name = "activityDateTime"
        graph_uri = (
            GRAPH_ENDPOINT_URI
            + 'beta/auditLogs/directoryAudits?$top=500&$filter=' + datetime_record_name
            + ' gt ' + last_datetime
        )
    elif auditlog_type == "signIns":
        datetime_record_name = "createdDateTime"
        graph_uri = (
            GRAPH_ENDPOINT_URI
            + 'beta/auditLogs/signIns?$top=500&$filter=' + datetime_record_name
            + ' gt ' + last_datetime
        )
    else:
        log("Unknown auditlog_type = " + auditlog_type)
        return

    max_record_datetime = last_datetime

    # May need to loop multiple times to get all of the data and retry throttled requestes with status code 429
    request_count = 0
    error_count = 0
    max_requests = 100
    max_errors = 50
    while request_count < max_requests and error_count < max_errors:
        request_count += 1

        # Issue Graph API request
        session = requests.Session()
        session.headers.update({'Authorization': "Bearer " + graph_token_response['accessToken']})
        response = session.get(graph_uri)
        content_length = len(response.content)
        response_json = response.json()

        log(
            "Get " + graph_uri + " returned status_code=" + str(response.status_code)
            + "; content_length=" + str(content_length)
            + "; requests=" + str(request_count) + "/" + str(max_requests)
            + "; errors=" + str(error_count) + "/" + str(max_errors)
        )

        if response.status_code != 200:
            error_count += 1
            log("*** ERROR ***")
            log("Headers: " + str(response.headers))
            log("Content: " + response.text)

            if response.status_code == 403:
                # Exit immediately
                log("Permission denied, existing.")
                return
            elif response.status_code == 429:
                # Pause for longer when throttled
                log("Request was throttled, waiting 10 seconds...")
                time.sleep(10.0)
                continue
            else:
                # Pause before retry
                log("Waiting 5 seconds...")
                time.sleep(5.0)
                continue

        # Check if received valid response
        if 'value' in response_json:
            count = len(response_json['value'])

            # Records are ordered in descending order by activityDateTime/createdDateTime, so first record is the
            # newest and last is the oldest
            if count > 0:
                last_record_datetime = response_json['value'][0][datetime_record_name]
                first_record_datetime = response_json['value'][count - 1][datetime_record_name]

                # Upload logs to blob storage
                blob_name = (
                    "logs_" + first_record_datetime.replace(":", "")
                    + "_" + last_record_datetime.replace(":", "")
                    + "_" + str(count)
                    + ".json"
                )
                blob_service.create_blob_from_text(
                    storage_container,
                    blob_name,
                    json.dumps(response_json),
                    encoding='utf-8', content_settings=ContentSettings(content_type='application/json')
                )

                log("Uploaded " + blob_name + " to " + storage_account + "/" + storage_container)

                if last_record_datetime > max_record_datetime:
                    max_record_datetime = last_record_datetime
            else:
                log("No new data")

            # If there is next page, go to next page. Otherwise, break out of the loop.
            if "@odata.nextLink" in response_json:
                graph_uri = response_json['@odata.nextLink']
                log("Next page found " + graph_uri)
            else:
                break

    # Record the last activityDateTime to filter next set of logs
    blob_service.set_container_metadata(
        storage_container,
        metadata={
            METADATA_LAST_DATETIME: max_record_datetime,
            METADATA_LAST_EXECUTION: str(datetime.now())
        }
    )
    log("Recorded new container last_datetime=" + max_record_datetime)


def main():
    # Set your Azure Active Directory application credentials.
    # Application must have permission for Microsoft.Graph AuditLog.Read.All
    # and RBAC role "Storage Blob Contributor" to the storage account.
    tenant_id = vault.decrypt_if_encrypted(envar='AAD_TENANT_ID')
    client_id = vault.decrypt_if_encrypted(envar='AAD_CLIENT_ID')
    client_secret = vault.decrypt_if_encrypted(envar='AAD_CLIENT_SECRET')
    storage_account = vault.decrypt_if_encrypted(envar='AAD_STORAGE_ACCOUNT')

    if not (tenant_id and client_id and client_secret and storage_account):
        print('[aad_auditlogs] missing required env var')
        return

    save_aad_auditlogs("directoryAudits", tenant_id, client_id, client_secret, storage_account, "logs-audit")

    # AAD signIns report is only available for Azure AD Premium P1 or higher and will return an error for non-premium
    # AAD tenants.
    save_aad_auditlogs("signIns", tenant_id, client_id, client_secret, storage_account, "logs-signin")


if __name__ == '__main__':
    main()
