from json import loads

from google.oauth2 import service_account
from googleapiclient.discovery import build

from vault import decrypt_if_encrypted


def process_row(
    service_name,
    service_version,
    resource_name,
    method,
    params,
    service_account_info=None,
    authorized_user_info=None,
    without_subject=False,
    subject=None,
    scopes=None,
):

    c = (
        service_account.Credentials.from_service_account_info(
            loads(decrypt_if_encrypted(service_account_info))
        )
        if authorized_user_info is None
        else service_account.Credentials.from_authorized_user_info(
            loads(decrypt_if_encrypted(authorized_user_info))
        )
    )
    if subject is None or without_subject:
        creds = c.with_scopes(scopes)
    else:
        creds = c.with_subject(subject).with_scopes(scopes)

    service = build(service_name, version=service_version, credentials=creds)
    resource_name, *subresource_names = resource_name.split('.')
    resource = getattr(service, resource_name)()
    for srn in subresource_names:
        resource = getattr(resource, srn)()

    return getattr(resource, method)(**loads(params)).execute()
