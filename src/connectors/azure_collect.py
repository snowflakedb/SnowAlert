"""Azure Inventory and Configuration
Load Inventory and Configuration of accounts using Service Principals
"""
from datetime import datetime
from dateutil.parser import parse as parse_date
import fire
import json
import re
import requests
from time import time
from urllib.parse import urlencode
import xmltodict

from azure.common.credentials import ServicePrincipalCredentials

from connectors.utils import updated, yaml_dump
from runners.utils import json_dumps
from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE


def access_token_cache(client_id, tenant, secret, resource, _creds={}):
    key = (client_id, tenant, secret, resource)
    cred = _creds.get(key)
    if cred is None or cred.token['expires_on'] - time() < 60:
        cred = _creds[key] = ServicePrincipalCredentials(
            client_id=client_id,
            tenant=tenant,
            secret=secret,
            resource=f'https://{resource}',
        )
    return cred.token['access_token']


CONNECTION_OPTIONS = [
    {
        'name': 'credentials',
        'title': "Azure Auditor Service Principals",
        'prompt': "JSON list of {client, tenant, secret, cloud} objects",
        'type': 'json',
        'placeholder': """[{"client": "...", "tenant": "...", "secret": "...", "cloud": "azure" | "usgov"}, ...]""",
        'required': True,
        'secret': True,
    }
]


# https://docs.microsoft.com/en-us/rest/api/resources/subscriptions/list#subscription
LANDING_TABLE_COLUMNS = [
    ('recorded_at', 'TIMESTAMP_LTZ'),
    ('tenant_id', 'VARCHAR(50)'),
    ('subscription_id', 'VARCHAR(50)'),
    ('id', 'VARCHAR(100)'),
    ('display_name', 'VARCHAR(500)'),
    ('state', 'VARCHAR(50)'),
    ('subscription_policies', 'VARIANT'),
    ('authorization_source', 'VARCHAR(50)'),
    ('managed_by_tenants', 'VARIANT'),
]


SUPPLEMENTARY_TABLES = {
    # https://docs.microsoft.com/en-us/graph/api/resources/credentialuserregistrationdetails#properties
    'reports_credential_user_registration_details': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('auth_methods', 'VARIANT'),
        ('id', 'STRING'),
        ('is_capable', 'BOOLEAN'),
        ('is_enabled', 'BOOLEAN'),
        ('is_mfa_registered', 'BOOLEAN'),
        ('is_registered', 'BOOLEAN'),
        ('user_display_name', 'STRING'),
        ('user_principal_name', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/resources/subscriptions/listlocations#location
    'subscriptions_locations': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('display_name', 'VARCHAR(500)'),
        ('id', 'VARCHAR(100)'),
        ('latitude', 'VARCHAR(100)'),
        ('longitude', 'VARCHAR(100)'),
        ('name', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/compute/virtualmachines/listall#virtualmachine
    'virtual_machines': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('identity', 'VARIANT'),
        ('location', 'VARCHAR(100)'),
        ('name', 'VARCHAR(100)'),
        ('plan', 'VARIANT'),
        ('properties', 'VARIANT'),
        ('resources', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
        ('zones', 'VARIANT'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/aks/managedclusters/list#response
    'managed_clusters': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('identity', 'VARCHAR(500)'),
        ('location', 'VARCHAR(500)'),
        ('name', 'VARCHAR(500)'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/keyvault/vaults/list
    'vaults': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('location', 'VARCHAR(500)'),
        ('name', 'VARCHAR(500)'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/keyvault/getkeys/getkeys#keyitem
    'vaults_keys': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('vault_name', 'VARCHAR(1000)'),
        ('error', 'VARIANT'),
        ('attributes', 'VARIANT'),
        ('kid', 'VARCHAR(1000)'),
        ('managed', 'BOOLEAN'),
        ('tags', 'VARIANT'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/keyvault/getsecrets/getsecrets#secretitem
    'vaults_secrets': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('vault_name', 'VARCHAR(1000)'),
        ('id', 'VARCHAR(500)'),
        ('error', 'VARIANT'),
        ('attributes', 'VARIANT'),
        ('content_type', 'VARCHAR(500)'),
        ('managed', 'BOOLEAN'),
        ('tags', 'VARIANT'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/resources/resourcegroups/list#resourcegroup
    'resource_groups': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('location', 'VARCHAR(500)'),
        ('managed_by', 'VARCHAR(1000)'),
        ('name', 'VARCHAR(1000)'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/appservice/appserviceenvironments/list#appserviceenvironmentresource
    'hosting_environments': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('kind', 'VARCHAR(500)'),
        ('location', 'VARCHAR(500)'),
        ('name', 'VARCHAR(1000)'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/appservice/appserviceenvironments/list#appserviceenvironmentresource
    'webapps': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('kind', 'VARCHAR(500)'),
        ('location', 'VARCHAR(500)'),
        ('name', 'VARCHAR(1000)'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/storagerp/storageaccounts/list#storageaccount
    'storage_accounts': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'VARCHAR(500)'),
        ('identity', 'VARIANT'),
        ('kind', 'VARCHAR(50)'),
        ('location', 'VARCHAR(500)'),
        ('name', 'VARCHAR(1000)'),
        ('properties', 'VARIANT'),
        ('sku', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'VARCHAR(1000)'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/storageservices/list-containers2
    'storage_accounts_containers': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('account_name', 'VARCHAR(1000)'),
        ('error', 'VARIANT'),
        ('name', 'VARCHAR(1000)'),
        ('properties', 'VARIANT'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/monitor/logprofiles/list#logprofileresource
    'log_profiles': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('identity', 'STRING'),
        ('kind', 'VARCHAR(50)'),
        ('location', 'STRING'),
        ('name', 'STRING'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/compute/disks/list#disk
    'disks': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('location', 'STRING'),
        ('managedBy', 'STRING'),
        ('name', 'STRING'),
        ('properties', 'VARIANT'),
        ('sku', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'STRING'),
        ('zones', 'VARIANT'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/authorization/roledefinitions/list#roledefinition
    'role_definitions': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('name', 'STRING'),
        ('properties', 'VARIANT'),
        ('type', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/virtualnetwork/networksecuritygroups/listall#networksecuritygroup
    'network_security_groups': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('etag', 'STRING'),
        ('name', 'STRING'),
        ('location', 'STRING'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/network-watcher/networkwatchers/listall#networkwatcher
    'network_watchers': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('etag', 'STRING'),
        ('id', 'STRING'),
        ('location', 'STRING'),
        ('name', 'STRING'),
        ('properties', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('type', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/securitycenter/pricings/list#pricingtier
    'pricings': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('name', 'STRING'),
        ('properties', 'VARIANT'),
        ('type', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/securitycenter/autoprovisioningsettings/list#autoprovisioningsettinglist
    'auto_provisioning_settings': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('name', 'STRING'),
        ('properties', 'VARIANT'),
        ('type', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/resources/policyassignments/list#policyassignment
    'policy_assignments': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('identity', 'VARIANT'),
        ('location', 'STRING'),
        ('name', 'STRING'),
        ('properties', 'VARIANT'),
        ('sku', 'VARIANT'),
        ('type', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/rest/api/securitycenter/securitycontacts/list#securitycontact
    'security_contacts': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('subscription_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('name', 'STRING'),
        ('type', 'STRING'),
        ('properties', 'VARIANT'),
    ],
    # https://docs.microsoft.com/en-us/graph/api/resources/serviceprincipal?view=graph-rest-beta#properties
    'service_principals': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('account_enabled', 'BOOLEAN'),
        ('alternative_names', 'VARIANT'),
        ('api', 'VARIANT'),
        ('app_display_name', 'STRING'),
        ('app_id', 'STRING'),
        ('app_role_assignment_required', 'BOOLEAN'),
        ('app_roles', 'VARIANT'),
        ('application_template_id', 'STRING'),
        ('app_owner_organization_id', 'STRING'),
        ('deleted', 'TIMESTAMP_LTZ'),
        ('display_name', 'STRING'),
        ('error_url', 'STRING'),
        ('homepage', 'STRING'),
        ('info', 'STRING'),
        ('login_url', 'STRING'),
        ('notification_email_addresses', 'VARIANT'),
        ('published_permission_scopes', 'VARIANT'),
        ('preferred_single_sign_on_mode', 'STRING'),
        ('preferred_token_signing_key_end', 'TIMESTAMP_LTZ'),
        ('saml_single_sign_on_settings', 'VARIANT'),
        ('service_principal_type', 'STRING'),
        ('add_ins', 'VARIANT'),
        ('sign_in_audience', 'STRING'),
        ('token_encryption_key_id', 'STRING'),
        ('key_credentials', 'VARIANT'),
        ('logout_url', 'STRING'),
        ('oauth2_permissions', 'VARIANT'),
        ('password_credentials', 'VARIANT'),
        ('preferred_token_signing_key_thumbprint', 'STRING'),
        ('publisher_name', 'STRING'),
        ('reply_urls', 'VARIANT'),
        ('saml_metadata_url', 'STRING'),
        ('service_principal_names', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('verified_publisher', 'VARIANT'),
    ],
    # https://docs.microsoft.com/en-us/graph/api/resources/group?view=graph-rest-1.0#properties
    'groups': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('allow_external_senders', 'BOOLEAN'),
        ('assigned_licenses', 'VARIANT'),
        ('auto_subscribe_new_members', 'BOOLEAN'),
        ('classification', 'STRING'),
        ('created', 'TIMESTAMP_LTZ'),
        ('creation_options', 'VARIANT'),
        ('deleted', 'TIMESTAMP_LTZ'),
        ('description', 'STRING'),
        ('display_name', 'STRING'),
        ('group_types', 'STRING'),
        ('has_members_with_license_errors', 'BOOLEAN'),
        ('id', 'STRING'),
        ('is_assignable_to_role', 'BOOLEAN'),
        ('is_subscribed_by_mail', 'BOOLEAN'),
        ('license_processing_state', 'STRING'),
        ('mail', 'STRING'),
        ('mail_enabled', 'BOOLEAN'),
        ('mail_nickname', 'STRING'),
        ('on_premises_last_sync', 'TIMESTAMP_LTZ'),
        ('on_premises_domain_name', 'STRING'),
        ('on_premises_net_bios_name', 'STRING'),
        ('on_premises_sam_account_name', 'STRING'),
        ('resource_behavior_options', 'VARIANT'),
        ('resource_provisioning_options', 'VARIANT'),
        ('on_premises_provisioning_errors', 'VARIANT'),
        ('on_premises_security_identifier', 'STRING'),
        ('on_premises_sync_enabled', 'BOOLEAN'),
        ('preferred_data_location', 'STRING'),
        ('proxy_addresses', 'STRING'),
        ('renewed', 'TIMESTAMP_LTZ'),
        ('security_enabled', 'BOOLEAN'),
        ('security_identifier', 'STRING'),
        ('unseen_count', 'NUMBER'),
        ('visibility', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/graph/api/resources/user?view=graph-rest-1.0#properties
    'users': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('about_me', 'STRING'),
        ('account_enabled', 'BOOLEAN'),
        ('age_group', 'STRING'),
        ('assigned_licenses', 'VARIANT'),
        ('assigned_plans', 'VARIANT'),
        ('birthday', 'TIMESTAMP_LTZ'),
        ('business_phones', 'STRING'),
        ('city', 'STRING'),
        ('company_name', 'STRING'),
        ('consent_provided_for_minor', 'STRING'),
        ('country', 'STRING'),
        ('created', 'TIMESTAMP_LTZ'),
        ('department', 'STRING'),
        ('display_name', 'STRING'),
        ('employee_id', 'STRING'),
        ('fax_number', 'STRING'),
        ('given_name', 'STRING'),
        ('hire_date', 'TIMESTAMP_LTZ'),
        ('id', 'STRING'),
        ('im_addresses', 'STRING'),
        ('interests', 'STRING'),
        ('is_resource_account', 'BOOLEAN'),
        ('job_title', 'STRING'),
        ('last_password_change', 'TIMESTAMP_LTZ'),
        ('legal_age_group_classification', 'STRING'),
        ('license_assignment_states', 'VARIANT'),
        ('mail', 'STRING'),
        ('mailbox_settings', 'VARIANT'),
        ('mail_nickname', 'STRING'),
        ('mobile_phone', 'STRING'),
        ('my_site', 'STRING'),
        ('office_location', 'STRING'),
        ('on_premises_distinguished_name', 'STRING'),
        ('on_premises_domain_name', 'STRING'),
        ('on_premises_extension_attributes', 'VARIANT'),
        ('on_premises_immutable_id', 'STRING'),
        ('on_premises_last_sync', 'TIMESTAMP_LTZ'),
        ('on_premises_provisioning_errors', 'VARIANT'),
        ('on_premises_sam_account_name', 'STRING'),
        ('on_premises_security_identifier', 'STRING'),
        ('on_premises_sync_enabled', 'BOOLEAN'),
        ('on_premises_user_principal_name', 'STRING'),
        ('other_mails', 'STRING'),
        ('password_policies', 'STRING'),
        ('password_profile', 'VARIANT'),
        ('past_projects', 'STRING'),
        ('postal_code', 'STRING'),
        ('preferred_data_location', 'STRING'),
        ('preferred_language', 'STRING'),
        ('preferred_name', 'STRING'),
        ('provisioned_plans', 'VARIANT'),
        ('proxy_addresses', 'STRING'),
        ('responsibilities', 'STRING'),
        ('schools', 'STRING'),
        ('show_in_address_list', 'BOOLEAN'),
        ('skills', 'STRING'),
        ('sign_in_sessions_valid_from', 'TIMESTAMP_LTZ'),
        ('state', 'STRING'),
        ('street_address', 'STRING'),
        ('surname', 'STRING'),
        ('usage_location', 'STRING'),
        ('user_principal_name', 'STRING'),
        ('user_type', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/graph/api/resources/intune-devices-manageddevice?view=graph-rest-1.0#properties
    'managed_devices': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('user_id', 'STRING'),
        ('device_name', 'STRING'),
        ('managed_device_owner_type', 'VARCHAR(100)'),
        ('device_action_results', 'VARIANT'),
        ('enrolled', 'TIMESTAMP_LTZ'),
        ('last_sync', 'TIMESTAMP_LTZ'),
        ('operating_system', 'STRING'),
        ('compliance_state', 'VARCHAR(100)'),
        ('jail_broken', 'STRING'),
        ('management_agent', 'VARCHAR(100)'),
        ('os_version', 'STRING'),
        ('eas_activated', 'BOOLEAN'),
        ('eas_device_id', 'STRING'),
        ('eas_activation', 'TIMESTAMP_LTZ'),
        ('azure_a_d_registered', 'BOOLEAN'),
        ('device_enrollment_type', 'VARCHAR(100)'),
        ('activation_lock_bypass_code', 'STRING'),
        ('email_address', 'STRING'),
        ('azure_a_d_device_id', 'STRING'),
        ('device_registration_state', 'VARCHAR(100)'),
        ('device_category_display_name', 'STRING'),
        ('is_supervised', 'BOOLEAN'),
        ('exchange_last_successful_sync', 'TIMESTAMP_LTZ'),
        ('exchange_access_state', 'VARCHAR(20)'),
        ('exchange_access_state_reason', 'VARCHAR(50)'),
        ('remote_assistance_session_url', 'STRING'),
        ('remote_assistance_session_error_details', 'STRING'),
        ('is_encrypted', 'BOOLEAN'),
        ('user_principal_name', 'STRING'),
        ('model', 'STRING'),
        ('manufacturer', 'STRING'),
        ('imei', 'STRING'),
        ('compliance_grace_period_expiration', 'TIMESTAMP_LTZ'),
        ('serial_number', 'STRING'),
        ('phone_number', 'STRING'),
        ('android_security_patch_level', 'STRING'),
        ('user_display_name', 'STRING'),
        ('configuration_manager_client_enabled_features', 'VARIANT'),
        ('wi_fi_mac_address', 'STRING'),
        ('device_health_attestation_state', 'VARIANT'),
        ('subscriber_carrier', 'STRING'),
        ('meid', 'STRING'),
        ('total_storage_space_in_bytes', 'NUMBER'),
        ('free_storage_space_in_bytes', 'NUMBER'),
        ('managed_device_name', 'STRING'),
        ('partner_reported_threat_state', 'VARCHAR(100)'),
    ],
}


def connect(connection_name, options):
    table_name_part = '' if connection_name == 'default' else f'_{connection_name}'
    table_prefix = f'azure_collect{table_name_part}'
    landing_table_name = f'data.{table_prefix}_connection'
    comment = yaml_dump(module='azure_collect', **options)

    db.create_table(
        name=landing_table_name,
        cols=LANDING_TABLE_COLUMNS,
        comment=comment,
        rw_role=SA_ROLE,
    )

    for table_postfix, cols in SUPPLEMENTARY_TABLES.items():
        supp_table = f'data.{table_prefix}_{table_postfix}'
        db.create_table(name=supp_table, cols=cols, rw_role=SA_ROLE)

    return {
        'newStage': 'finalized',
        'newMessage': 'Azure Collect landing tables created.',
    }


API_SPECS = {
    'subscriptions': {
        'request': {'path': '/subscriptions', 'api-version': '2019-06-01'},
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'error': 'error',
            'id': 'id',
            'subscriptionId': 'subscription_id',
            'displayName': 'display_name',
            'state': 'state',
            'subscriptionPolicies': 'subscription_policies',
            'authorizationSource': 'authorization_source',
            'managedByTenants': 'managed_by_tenants',
        },
    },
    'reports_credential_user_registration_details': {
        'request': {
            'path': '/beta/reports/credentialUserRegistrationDetails',
            'host': {'usgov': 'graph.microsoft.us', 'azure': 'graph.microsoft.com'},
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'error': 'error',
            'id': 'id',
            'userPrincipalName': 'user_principal_name',
            'userDisplayName': 'user_display_name',
            'authMethods': 'auth_methods',
            'isRegistered': 'is_registered',
            'isEnabled': 'is_enabled',
            'isCapable': 'is_capable',
            'isMfaRegistered': 'is_mfa_registered',
        },
    },
    'service_principals': {
        'request': {
            'path': '/beta/servicePrincipals',
            'host': {'usgov': 'graph.microsoft.us', 'azure': 'graph.microsoft.com'},
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'error': 'error',
            'id': 'id',
            'api': 'api',
            'accountEnabled': 'account_enabled',
            'alternativeNames': 'alternative_names',
            'appDisplayName': 'app_display_name',
            'appId': 'app_id',
            'appRoleAssignmentRequired': 'app_role_assignment_required',
            'appRoles': 'app_roles',
            'applicationTemplateId': 'application_template_id',
            'appOwnerOrganizationId': 'app_owner_organization_id',
            'deletedDateTime': 'deleted',
            'displayName': 'display_name',
            'errorUrl': 'error_url',
            'homepage': 'homepage',
            'loginUrl': 'login_url',
            'notificationEmailAddresses': 'notification_email_addresses',
            'publishedPermissionScopes': 'published_permission_scopes',
            'preferredSingleSignOnMode': 'preferred_single_sign_on_mode',
            'preferredTokenSigningKeyEndDateTime': 'preferred_token_signing_key_end',
            'samlSingleSignOnSettings': 'saml_single_sign_on_settings',
            'servicePrincipalType': 'service_principal_type',
            'signInAudience': 'sign_in_audience',
            'tokenEncryptionKeyId': 'token_encryption_key_id',
            'addIns': 'add_ins',
            'info': 'info',
            'keyCredentials': 'key_credentials',
            'logoutUrl': 'logout_url',
            'oauth2Permissions': 'oauth2_permissions',
            'passwordCredentials': 'password_credentials',
            'preferredTokenSigningKeyThumbprint': 'preferred_token_signing_key_thumbprint',
            'publisherName': 'publisher_name',
            'replyUrls': 'reply_urls',
            'samlMetadataUrl': 'saml_metadata_url',
            'servicePrincipalNames': 'service_principal_names',
            'tags': 'tags',
            'verifiedPublisher': 'verified_publisher',
        },
    },
    'groups': {
        'request': {
            'path': '/v1.0/groups',
            'host': {'usgov': 'graph.microsoft.us', 'azure': 'graph.microsoft.com'},
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'error': 'error',
            'allowExternalSenders': 'allow_external_senders',
            'assignedLicenses': 'assigned_licenses',
            'autoSubscribeNewMembers': 'auto_subscribe_new_members',
            'classification': 'classification',
            'createdDateTime': 'created',
            'creationOptions': 'creation_options',
            'deletedDateTime': 'deleted',
            'description': 'description',
            'displayName': 'display_name',
            'groupTypes': 'group_types',
            'hasMembersWithLicenseErrors': 'has_members_with_license_errors',
            'id': 'id',
            'isAssignableToRole': 'is_assignable_to_role',
            'isSubscribedByMail': 'is_subscribed_by_mail',
            'licenseProcessingState': 'license_processing_state',
            'mail': 'mail',
            'mailEnabled': 'mail_enabled',
            'mailNickname': 'mail_nickname',
            'onPremisesDomainName': 'on_premises_domain_name',
            'onPremisesNetBiosName': 'on_premises_net_bios_name',
            'onPremisesSamAccountName': 'on_premises_sam_account_name',
            'resourceBehaviorOptions': 'resource_behavior_options',
            'resourceProvisioningOptions': 'resource_provisioning_options',
            'onPremisesLastSyncDateTime': 'on_premises_last_sync',
            'onPremisesProvisioningErrors': 'on_premises_provisioning_errors',
            'onPremisesSecurityIdentifier': 'on_premises_security_identifier',
            'onPremisesSyncEnabled': 'on_premises_sync_enabled',
            'preferredDataLocation': 'preferred_data_location',
            'proxyAddresses': 'proxy_addresses',
            'renewedDateTime': 'renewed',
            'securityEnabled': 'security_enabled',
            'securityIdentifier': 'security_identifier',
            'unseenCount': 'unseen_count',
            'visibility': 'visibility',
        },
    },
    'users': {
        'request': {
            'path': '/v1.0/users',
            'host': {'usgov': 'graph.microsoft.us', 'azure': 'graph.microsoft.com'},
            'params': {
                '$select': (
                    'accountEnabled,'
                    'businessPhones,'
                    'city,'
                    'country,'
                    'createdDateTime,'
                    'department,'
                    'displayName,'
                    'employeeId,'
                    'faxNumber,'
                    'givenName,'
                    'jobTitle,'
                    'mail,'
                    'id,'
                    'lastPasswordChangeDateTime,'
                    'mobilePhone,'
                    'officeLocation,'
                    'preferredLanguage,'
                    'surname,'
                    'passwordPolicies,'
                    'passwordProfile,'
                    'userPrincipalName,'
                    'userType'
                )
            },
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'error': 'error',
            'aboutMe': 'about_me',
            'accountEnabled': 'account_enabled',
            'ageGroup': 'age_group',
            'assignedLicenses': 'assigned_licenses',
            'assignedPlans': 'assigned_plans',
            'birthday': 'birthday',
            'businessPhones': 'business_phones',
            'city': 'city',
            'companyName': 'company_name',
            'consentProvidedForMinor': 'consent_provided_for_minor',
            'country': 'country',
            'createdDateTime': 'created',
            'department': 'department',
            'displayName': 'display_name',
            'employeeId': 'employee_id',
            'faxNumber': 'fax_number',
            'givenName': 'given_name',
            'hireDate': 'hire_date',
            'id': 'id',
            'imAddresses': 'im_addresses',
            'interests': 'interests',
            'isResourceAccount': 'is_resource_account',
            'jobTitle': 'job_title',
            'lastPasswordChangeDateTime': 'last_password_change',
            'legalAgeGroupClassification': 'legal_age_group_classification',
            'licenseAssignmentStates': 'license_assignment_states',
            'mail': 'mail',
            'mailboxSettings': 'mailbox_settings',
            'mailNickname': 'mail_nickname',
            'mobilePhone': 'mobile_phone',
            'mySite': 'my_site',
            'officeLocation': 'office_location',
            'onPremisesDistinguishedName': 'on_premises_distinguished_name',
            'onPremisesDomainName': 'on_premises_domain_name',
            'onPremisesExtensionAttributes': 'on_premises_extension_attributes',
            'onPremisesImmutableId': 'on_premises_immutable_id',
            'onPremisesLastSyncDateTime': 'on_premises_last_sync',
            'onPremisesProvisioningErrors': 'on_premises_provisioning_errors',
            'onPremisesSamAccountName': 'on_premises_sam_account_name',
            'onPremisesSecurityIdentifier': 'on_premises_security_identifier',
            'onPremisesSyncEnabled': 'on_premises_sync_enabled',
            'onPremisesUserPrincipalName': 'on_premises_user_principal_name',
            'otherMails': 'other_mails',
            'passwordPolicies': 'password_policies',
            'passwordProfile': 'password_profile',
            'pastProjects': 'past_projects',
            'postalCode': 'postal_code',
            'preferredDataLocation': 'preferred_data_location',
            'preferredLanguage': 'preferred_language',
            'preferredName': 'preferred_name',
            'provisionedPlans': 'provisioned_plans',
            'proxyAddresses': 'proxy_addresses',
            'responsibilities': 'responsibilities',
            'schools': 'schools',
            'showInAddressList': 'show_in_address_list',
            'skills': 'skills',
            'signInSessionsValidFromDateTime': 'sign_in_sessions_valid_from',
            'state': 'state',
            'streetAddress': 'street_address',
            'surname': 'surname',
            'usageLocation': 'usage_location',
            'userPrincipalName': 'user_principal_name',
            'userType': 'user_type',
        },
    },
    'managed_devices': {
        'request': {
            'path': '/v1.0/deviceManagement/managedDevices',
            'host': {'usgov': 'graph.microsoft.us', 'azure': 'graph.microsoft.com'},
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'error': 'error',
            'id': 'id',
            'userId': 'user_id',
            'deviceName': 'device_name',
            'managedDeviceOwnerType': 'managed_device_owner_type',
            'deviceActionResults': 'device_action_results',
            'enrolledDateTime': 'enrolled',
            'lastSyncDateTime': 'last_sync',
            'operatingSystem': 'operating_system',
            'complianceState': 'compliance_state',
            'jailBroken': 'jail_broken',
            'managementAgent': 'management_agent',
            'osVersion': 'os_version',
            'easActivated': 'eas_activated',
            'easDeviceId': 'eas_device_id',
            'easActivationDateTime': 'eas_activation',
            'azureADRegistered': 'azure_a_d_registered',
            'deviceEnrollmentType': 'device_enrollment_type',
            'activationLockBypassCode': 'activation_lock_bypass_code',
            'emailAddress': 'email_address',
            'azureADDeviceId': 'azure_a_d_device_id',
            'deviceRegistrationState': 'device_registration_state',
            'deviceCategoryDisplayName': 'device_category_display_name',
            'isSupervised': 'is_supervised',
            'exchangeLastSuccessfulSyncDateTime': 'exchange_last_successful_sync',
            'exchangeAccessState': 'exchange_access_state',
            'exchangeAccessStateReason': 'exchange_access_state_reason',
            'remoteAssistanceSessionUrl': 'remote_assistance_session_url',
            'remoteAssistanceSessionErrorDetails': 'remote_assistance_session_error_details',
            'isEncrypted': 'is_encrypted',
            'userPrincipalName': 'user_principal_name',
            'model': 'model',
            'manufacturer': 'manufacturer',
            'imei': 'imei',
            'complianceGracePeriodExpirationDateTime': 'compliance_grace_period_expiration',
            'serialNumber': 'serial_number',
            'phoneNumber': 'phone_number',
            'androidSecurityPatchLevel': 'android_security_patch_level',
            'userDisplayName': 'user_display_name',
            'configurationManagerClientEnabledFeatures': 'configuration_manager_client_enabled_features',
            'wiFiMacAddress': 'wi_fi_mac_address',
            'deviceHealthAttestationState': 'device_health_attestation_state',
            'subscriberCarrier': 'subscriber_carrier',
            'meid': 'meid',
            'totalStorageSpaceInBytes': 'total_storage_space_in_bytes',
            'freeStorageSpaceInBytes': 'free_storage_space_in_bytes',
            'managedDeviceName': 'managed_device_name',
            'partnerReportedThreatState': 'partner_reported_threat_state',
        },
    },
    'subscriptions_locations': {
        'request': {
            'path': '/subscriptions/{subscriptionId}/locations',
            'api-version': '2019-06-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'displayName': 'display_name',
            'id': 'id',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'name': 'name',
        },
    },
    'virtual_machines': {
        'request': {
            'path': '/subscriptions/{subscriptionId}/providers/Microsoft.Compute/virtualMachines',
            'api-version': '2019-03-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'identity': 'identity',
            'location': 'location',
            'name': 'name',
            'plan': 'plan',
            'properties': 'properties',
            'resources': 'resources',
            'tags': 'tags',
            'type': 'type',
            'zones': 'zones',
        },
    },
    'managed_clusters': {
        'request': {
            'path': '/subscriptions/{subscriptionId}/providers/Microsoft.ContainerService/managedClusters',
            'api-version': '2019-08-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'identity': 'identity',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'vaults': {
        'request': {
            'path': '/subscriptions/{subscriptionId}/resources',
            'params': {'$filter': 'resourceType eq \'Microsoft.KeyVault/vaults\''},
            'api-version': '2019-11-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'location': 'location',
            'name': 'name',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'vaults_keys': {
        'request': {
            'host': {
                'azure': '{vaultName}.vault.azure.net',
                'usgov': '{vaultName}.vault.usgovcloudapi.net',
            },
            'auth_audience': {
                'azure': 'vault.azure.net',
                'usgov': 'vault.usgovcloudapi.net',
            },
            'path': '/keys',
            'params': {'maxresults': '25'},
            'api-version': '7.0',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'vaultName': 'vault_name',
            'error': 'error',
            'attributes': 'attributes',
            'kid': 'kid',
            'managed': 'managed',
            'tags': 'tags',
        },
    },
    'vaults_secrets': {
        'request': {
            'host': {
                'azure': '{vaultName}.vault.azure.net',
                'usgov': '{vaultName}.vault.usgovcloudapi.net',
            },
            'auth_audience': {
                'azure': 'vault.azure.net',
                'usgov': 'vault.usgovcloudapi.net',
            },
            'path': '/secrets',
            'params': {'maxresults': '25'},
            'api-version': '7.0',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'vaultName': 'vault_name',
            'error': 'error',
            'attributes': 'attributes',
            'contentType': 'content_type',
            'id': 'id',
            'managed': 'managed',
            'tags': 'tags',
        },
    },
    'resource_groups': {
        'request': {
            'path': '/subscriptions/{subscriptionId}/resourcegroups',
            'api-version': '2019-08-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'location': 'location',
            'managedBy': 'managed_by',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'hosting_environments': {
        'request': {
            'path': '/subscriptions/{subscriptionId}/providers/Microsoft.Web/hostingEnvironments',
            'api-version': '2019-08-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'kind': 'kind',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'webapps': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/resourceGroups/{resourceGroupName}'
                '/providers/Microsoft.Web/hostingEnvironments/{name}'
                '/sites'
            ),
            'api-version': '2019-08-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'kind': 'kind',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'storage_accounts': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/providers/Microsoft.Storage/storageAccounts'
            ),
            'api-version': '2019-06-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'identity': 'identity',
            'kind': 'kind',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'sku': 'sku',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'storage_accounts_containers': {
        'request': {
            'path': '/',
            'params': {'comp': 'list'},
            'host': {
                'azure': '{accountName}.blob.core.windows.net',
                'usgov': '{accountName}.blob.core.usgovcloudapi.net',
            },
            'auth_audience': 'storage.azure.com',
            'api-version': '2019-02-02',
        },
        'response_value_key': 'EnumerationResults.Containers.Container',
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'accountName': 'account_name',
            'error': 'error',
            'Error': 'error',  # upper case because response is XML
            'Name': 'name',
            'Properties': 'properties',
        },
    },
    'log_profiles': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/providers/microsoft.insights/logprofiles'
            ),
            'api-version': '2016-03-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'identity': 'identity',
            'kind': 'kind',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'disks': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}/providers/Microsoft.Compute/disks'
            ),
            'api-version': '2019-07-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'location': 'location',
            'managedBy': 'managedBy',
            'name': 'name',
            'properties': 'properties',
            'sku': 'sku',
            'tags': 'tags',
            'type': 'type',
            'zones': 'zones',
        },
    },
    'role_definitions': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/providers/Microsoft.Authorization/roleDefinitions'
            ),
            'api-version': '2015-07-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'displayName': 'display_name',
            'id': 'id',
            'name': 'name',
            'properties': 'properties',
            'type': 'type',
        },
    },
    'network_security_groups': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/providers/Microsoft.Network/networkSecurityGroups'
            ),
            'api-version': '2019-09-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'etag': 'etag',
            'id': 'id',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'network_watchers': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/providers/Microsoft.Network/networkWatchers'
            ),
            'api-version': '2019-09-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'etag': 'etag',
            'id': 'id',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'tags': 'tags',
            'type': 'type',
        },
    },
    'pricings': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/providers/Microsoft.Security/pricings'
            ),
            'api-version': '2018-06-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'name': 'name',
            'properties': 'properties',
            'type': 'type',
        },
    },
    'auto_provisioning_settings': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/providers/Microsoft.Security/autoProvisioningSettings'
            ),
            'api-version': '2017-08-01-preview',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'name': 'name',
            'properties': 'properties',
            'type': 'type',
        },
    },
    'policy_assignments': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/providers/Microsoft.Authorization/policyAssignments'
            ),
            'api-version': '2019-09-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'identity': 'identity',
            'location': 'location',
            'name': 'name',
            'properties': 'properties',
            'sku': 'sku',
            'type': 'type',
        },
    },
    'security_contacts': {
        'request': {
            'path': (
                '/subscriptions/{subscriptionId}'
                '/providers/Microsoft.Security/securityContacts'
            ),
            'api-version': '2017-08-01-preview',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'id': 'id',
            'name': 'name',
            'type': 'type',
            'properties': 'properties',
        },
    },
}


def GET(kind, params, cred):
    spec = API_SPECS[kind]
    request_spec = spec['request']
    path = request_spec['path'].format(**params)
    cloud = (
        cred['cloud'] if 'cloud' in cred else ('usgov' if cred.get('gov') else 'azure')
    )

    host = request_spec.get(
        'host',
        {'azure': 'management.azure.com', 'usgov': 'management.usgovcloudapi.net'},
    )
    if type(host) is dict:
        host = host[cloud]
    host = host.format(**params)
    auth_aud = request_spec.get('auth_audience', host)
    if type(auth_aud) is dict:
        auth_aud = auth_aud[cloud]
    api_version = request_spec.get('api-version')
    query_params = urlencode(
        updated(
            {'api-version': api_version} if api_version else {},
            request_spec.get('params'),
        )
    )
    bearer_token = access_token_cache(
        cred['client'], cred['tenant'], cred['secret'], auth_aud
    )
    url = f'https://{host}{path}' + (f'?{query_params}' if query_params else '')
    log.debug(f'GET {url}')

    nextUrl = url
    values = []
    while nextUrl:
        response = requests.get(
            nextUrl,
            headers={
                'Authorization': 'Bearer ' + bearer_token,
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'x-ms-version': api_version,
            },
        )

        log.debug(f'<- {response.status_code}')

        try:
            result = json.loads(
                json_dumps(xmltodict.parse(response.text.encode()))
                if response.text.startswith('<?xml')
                else response.text
            )

        except json.JSONDecodeError:
            result = {
                'error_type': 'JSONDecodeError',
                'status_code': response.status_code,
                'text': response.text,
            }

        # empty lists of values are recorded as empty rows
        # error values are recorded as rows with error and empty value cols
        # normal values are recorded with populated values and an empty error col
        def response_values(result):
            for vk in spec.get('response_value_key', 'value').split('.'):
                if result is None or vk not in result:
                    break
                result = result[vk]

            return (
                result
                if type(result) is list
                else [result]
                if type(result) is dict
                else [{'error': result}]
            ) or [{}]

        values += [
            updated(
                {},
                v,
                params,
                headerDate=parse_date(response.headers['Date']),
                tenantId=cred['tenant'],
            )
            for v in response_values(result)
        ]

        if 'nextLink' in result:
            nextUrl = result['nextLink']
            continue

        if '@odata.nextLink' in result:
            nextUrl = result['@odata.nextLink']
            continue

        if 'EnumerationResults' in result:
            nextMarker = result['EnumerationResults'].get('NextMarker')
            if nextMarker:
                nextUrl = re.sub(r'(&marker=.*)?$', f'&marker={nextMarker}', nextUrl)
                continue

        break

    return [{spec['response'][k]: v for k, v in x.items()} for x in values]


def ingest(table_name, options, dryrun=False):
    connection_name = options['name']
    schedule = options.get('schedule', '*')

    if schedule not in ['*', '0 *']:
        log.info('not time yet')
        return 0

    if schedule == '0 *':  # hourly
        # todo(anf): robust cron using run metadata
        now = datetime.now()
        if now.minute > 15:
            log.info('not time yet')
            return 0

    num_loaded = 0
    for cred in options['credentials']:
        table_name_part = '' if connection_name == 'default' else f'_{connection_name}'
        table_prefix = f'data.azure_collect{table_name_part}'

        def load_table(kind, **params):
            nonlocal num_loaded
            values = db.retry(
                f=lambda: GET(kind, params, cred=cred),
                E=(requests.exceptions.SSLError, requests.exceptions.ConnectionError),
                n=10,
                sleep_seconds_btw_retry=30,
            )
            kind = 'connection' if kind == 'subscriptions' else kind
            table_name = f'{table_prefix}_{kind}'
            result = db.insert(table_name, values, dryrun=dryrun)
            num_loaded += len(values)
            log.info(f'-> {table_name} {result}')
            return values

        load_table('users')
        load_table('groups')
        load_table('service_principals')
        load_table('reports_credential_user_registration_details')
        load_table('managed_devices')

        for s in load_table('subscriptions'):
            sid = s.get('subscription_id')
            if sid is None:
                log.debug(f'subscription without id: {s}')
                continue

            load_table('pricings', subscriptionId=sid)
            load_table('auto_provisioning_settings', subscriptionId=sid)
            load_table('policy_assignments', subscriptionId=sid)
            load_table('security_contacts', subscriptionId=sid)

            load_table('virtual_machines', subscriptionId=sid)

            load_table('role_definitions', subscriptionId=sid)
            load_table('network_watchers', subscriptionId=sid)
            load_table('network_security_groups', subscriptionId=sid)

            load_table('disks', subscriptionId=sid)
            load_table('log_profiles', subscriptionId=sid)

            for henv in load_table('hosting_environments', subscriptionId=sid):
                if 'properties' in henv:
                    rg_name = henv['properties']['resourceGroup']
                    load_table(
                        'webapps',
                        subscriptionId=sid,
                        resourceGroupName=rg_name,
                        name=henv['name'],
                    )

            for sa in load_table('storage_accounts', subscriptionId=sid):
                if 'name' in sa:
                    load_table(
                        'storage_accounts_containers',
                        subscriptionId=sid,
                        accountName=sa['name'],
                    )

            for rg in load_table('resource_groups', subscriptionId=sid):
                if 'name' in rg:
                    pass

            load_table('subscriptions_locations', subscriptionId=sid)
            load_table('managed_clusters', subscriptionId=sid)
            for v in load_table('vaults', subscriptionId=sid):
                if 'name' in v:
                    load_table('vaults_keys', vaultName=v['name'])
                    load_table('vaults_secrets', vaultName=v['name'])

    return num_loaded


def main(table_name, tenant, client, secret, cloud, dryrun=True, run_now=False):
    now = datetime.now()
    schedule = '*' if run_now or (now.hour % 3 == 1 and now.minute < 15) else False

    ingest(
        table_name,
        {
            'name': 'default',
            'credentials': [
                {'tenant': tenant, 'client': client, 'secret': secret, 'cloud': cloud}
            ],
            'schedule': schedule,
        },
        dryrun=dryrun,
    )


if __name__ == '__main__':
    fire.Fire(main)
