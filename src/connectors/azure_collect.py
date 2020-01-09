"""Azure Inventory and Configuration
Load Inventory and Configuration of accounts using Service Principals
"""

from collections import defaultdict
from dateutil.parser import parse as parse_date
import fire
import json
import requests
from urllib.parse import urlencode
import xmltodict

from azure.common.credentials import ServicePrincipalCredentials

from connectors.utils import updated, yaml_dump
from runners.utils import json_dumps
from runners.helpers import db, log
from runners.helpers.dbconfig import ROLE as SA_ROLE


CLIENT = ''
TENANT = ''
SECRET = ''


class KeyedDefaultDict(defaultdict):
    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        else:
            ret = self[key] = self.default_factory(key)
            return ret


CREDS = KeyedDefaultDict(  # type: ignore
    lambda xs: ServicePrincipalCredentials(  # type: ignore
        client_id=xs[0], tenant=xs[1], secret=xs[2], resource=f'https://{xs[3]}'
    )
)


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
        ('subscription_id', 'VARCHAR(50)'),
    ],
    # https://docs.virtual_machinesmicrosoft.com/en-us/rest/api/compute/virtualmachines/listall#virtualmachine
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
        ('subscription_id', 'VARCHAR(50)'),
        ('kid', 'VARCHAR(500)'),
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
        ('subscription_id', 'VARCHAR(50)'),
        ('id', 'VARCHAR(500)'),
        ('error', 'VARIANT'),
        ('attributes', 'VARIANT'),
        ('kid', 'VARCHAR(1000)'),
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
    # https://docs.microsoft.com/en-us/rest/api/network-watcher/networkwatchers/getflowlogstatus#flowloginformation
    'network_watcher_flow_log_status': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('subscription_id', 'VARCHAR(50)'),
        ('network_watcher_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('flow_analytics_configuration', 'VARIANT'),
        ('properties', 'VARIANT'),
        ('target_resource_id', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/graph/api/resources/serviceprincipal?view=graph-rest-beta#properties
    'service_principals': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('id', 'STRING'),
        ('account_enabled', 'BOOLEAN'),
        ('app_display_name', 'STRING'),
        ('app_id', 'STRING'),
        ('app_role_assignment_required', 'BOOLEAN'),
        ('app_roles', 'VARIANT'),
        ('display_name', 'STRING'),
        ('error_url', 'STRING'),
        ('homepage', 'STRING'),
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
        ('deleted', 'TIMESTAMP_LTZ'),
        ('description', 'STRING'),
        ('display_name', 'STRING'),
        ('group_types', 'STRING'),
        ('has_members_with_license_errors', 'BOOLEAN'),
        ('id', 'STRING'),
        ('is_subscribed_by_mail', 'BOOLEAN'),
        ('license_processing_state', 'STRING'),
        ('mail', 'STRING'),
        ('mail_enabled', 'BOOLEAN'),
        ('mail_nickname', 'STRING'),
        ('on_premises_last_sync', 'TIMESTAMP_LTZ'),
        ('on_premises_provisioning_errors', 'VARIANT'),
        ('on_premises_security_identifier', 'STRING'),
        ('on_premises_sync_enabled', 'BOOLEAN'),
        ('preferred_data_location', 'STRING'),
        ('proxy_addresses', 'STRING'),
        ('renewed', 'TIMESTAMP_LTZ'),
        ('security_enabled', 'BOOLEAN'),
        ('security_identifier', 'STRING'),
        ('unseen_count', 'INT32'),
        ('visibility', 'STRING'),
    ],
    # https://docs.microsoft.com/en-us/graph/api/resources/user?view=graph-rest-1.0#properties
    'users': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('tenant_id', 'VARCHAR(50)'),
        ('error', 'VARIANT'),
        ('account_enabled', 'BOOLEAN'),
        ('age_group', 'STRING'),
        ('assigned_licenses', 'VARIANT'),
        ('assigned_plans', 'VARIANT'),
        ('city', 'STRING'),
        ('company_name', 'STRING'),
        ('consent_provided_for_minor', 'STRING'),
        ('country', 'STRING'),
        ('created', 'DATETIME_LTZ'),
        ('creation_type', 'STRING'),
        ('deletion_timestamp', 'STRING'),
        ('department', 'STRING'),
        ('dir_sync_enabled', 'STRING'),
        ('display_name', 'STRING'),
        ('employee_id', 'STRING'),
        ('facsimile_telephone_number', 'STRING'),
        ('given_name', 'STRING'),
        ('immutable_id', 'STRING'),
        ('is_compromised', 'STRING'),
        ('job_title', 'STRING'),
        ('last_dir_sync_time', 'STRING'),
        ('legal_age_group_classification', 'STRING'),
        ('mail', 'STRING'),
        ('mail_nickname', 'STRING'),
        ('mobile', 'STRING'),
        ('object_id', 'STRING'),
        ('object_type', 'STRING'),
        ('odata_type', 'STRING'),
        ('on_premises_distinguished_name', 'STRING'),
        ('on_premises_security_identifier', 'STRING'),
        ('other_mails', 'VARIANT'),
        ('password_policies', 'STRING'),
        ('password_profile', 'VARIANT'),
        ('physical_delivery_office_name', 'STRING'),
        ('postal_code', 'STRING'),
        ('preferred_language', 'STRING'),
        ('provisioned_plans', 'VARIANT'),
        ('provisioning_errors', 'VARIANT'),
        ('proxy_addresses', 'VARIANT'),
        ('refresh_tokens_valid_from', 'DATETIME_LTZ'),
        ('show_in_address_list', 'STRING'),
        ('sign_in_names', 'VARIANT'),
        ('sip_proxy_address', 'STRING'),
        ('state', 'STRING'),
        ('street_address', 'STRING'),
        ('surname', 'STRING'),
        ('telephone_number', 'STRING'),
        ('thumbnail_photo_odata_media_edit_link', 'STRING'),
        ('usage_location', 'STRING'),
        ('user_identities', 'VARIANT'),
        ('user_principal_name', 'STRING'),
        ('user_state', 'STRING'),
        ('user_state_changed_on', 'STRING'),
        ('user_type', 'STRING'),
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
        'children': [
            {
                'name': [
                    'subscriptions_locations',
                    'managed_clusters',
                    'virtual_machines',
                    'vaults',
                ],
                'args': {'subscriptionId': 'subscription_id'},
            }
        ],
    },
    'reports_credential_user_registration_details': {
        'request': {
            'path': '/beta/reports/credentialUserRegistrationDetails',
            'host': 'graph.microsoft.com',
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
        'request': {'path': '/beta/servicePrincipals', 'host': 'graph.microsoft.com'},
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'error': 'error',
            'id': 'id',
            'accountEnabled': 'account_enabled',
            'appDisplayName': 'app_display_name',
            'appId': 'app_id',
            'appRoleAssignmentRequired': 'app_role_assignment_required',
            'appRoles': 'app_roles',
            'displayName': 'display_name',
            'errorUrl': 'error_url',
            'homepage': 'homepage',
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
        },
    },
    'groups': {
        'request': {
            'path': '/v1.0/{subscriptionId}/groups',
            'host': 'graph.microsoft.com',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'allowExternalSenders': 'allow_external_senders',
            'assignedLicenses': 'assigned_licenses',
            'autoSubscribeNewMembers': 'auto_subscribe_new_members',
            'classification': 'classification',
            'createdDateTime': 'created',
            'deletedDateTime': 'deleted',
            'description': 'description',
            'displayName': 'display_name',
            'groupTypes': 'group_types',
            'hasMembersWithLicenseErrors': 'has_members_with_license_errors',
            'id': 'id',
            'isSubscribedByMail': 'is_subscribed_by_mail',
            'licenseProcessingState': 'license_processing_state',
            'mail': 'mail',
            'mailEnabled': 'mail_enabled',
            'mailNickname': 'mail_nickname',
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
            'path': '/v1.0/{subscriptionId}/users',
            'host': 'graph.microsoft.com',
        },
        'response': {
            'headerDate': 'recorded_at',
            'tenantId': 'tenant_id',
            'subscriptionId': 'subscription_id',
            'error': 'error',
            'accountEnabled': 'account_enabled',
            'ageGroup': 'age_group',
            'assignedLicenses': 'assigned_licenses',
            'assignedPlans': 'assigned_plans',
            'city': 'city',
            'companyName': 'company_name',
            'consentProvidedForMinor': 'consent_provided_for_minor',
            'country': 'country',
            'createdDateTime': 'created',
            'creationType': 'creation_type',
            'deletionTimestamp': 'deletion_timestamp',
            'department': 'department',
            'dirSyncEnabled': 'dir_sync_enabled',
            'displayName': 'display_name',
            'employeeId': 'employee_id',
            'facsimileTelephoneNumber': 'facsimile_telephone_number',
            'givenName': 'given_name',
            'immutableId': 'immutable_id',
            'isCompromised': 'is_compromised',
            'jobTitle': 'job_title',
            'lastDirSyncTime': 'last_dir_sync_time',
            'legalAgeGroupClassification': 'legal_age_group_classification',
            'mail': 'mail',
            'mailNickname': 'mail_nickname',
            'mobile': 'mobile',
            'objectId': 'object_id',
            'objectType': 'object_type',
            'odata.type': 'odata_type',
            'onPremisesDistinguishedName': 'on_premises_distinguished_name',
            'onPremisesSecurityIdentifier': 'on_premises_security_identifier',
            'otherMails': 'other_mails',
            'passwordPolicies': 'password_policies',
            'passwordProfile': 'password_profile',
            'physicalDeliveryOfficeName': 'physical_delivery_office_name',
            'postalCode': 'postal_code',
            'preferredLanguage': 'preferred_language',
            'provisionedPlans': 'provisioned_plans',
            'provisioningErrors': 'provisioning_errors',
            'proxyAddresses': 'proxy_addresses',
            'refreshTokensValidFromDateTime': 'refresh_tokens_valid_from',
            'showInAddressList': 'show_in_address_list',
            'signInNames': 'sign_in_names',
            'sipProxyAddress': 'sip_proxy_address',
            'state': 'state',
            'streetAddress': 'street_address',
            'surname': 'surname',
            'telephoneNumber': 'telephone_number',
            'thumbnailPhoto@odata.mediaEditLink': 'thumbnail_photo_odata_media_edit_link',
            'usageLocation': 'usage_location',
            'userIdentities': 'user_identities',
            'userPrincipalName': 'user_principal_name',
            'userState': 'user_state',
            'userStateChangedOn': 'user_state_changed_on',
            'userType': 'user_type',
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
        'children': [{'name': 'vaults_keys', 'args': {'vaultName': 'name'}}],
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
            'subscriptionId': 'subscription_id',
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
            'subscriptionId': 'subscription_id',
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
            'vaultName': 'vault_name',
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
            'vaultName': 'vault_name',
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
            'vaultName': 'vault_name',
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
    'network_watcher_flow_log_status': {
        'request': {
            'method': 'POST',
            'path': (
                '{networkWatcherId}/queryFlowLogStatus'
            ),
            'api-version': '2019-09-01',
        },
        'response': {
            'headerDate': 'recorded_at',
            'subscriptionId': 'subscription_id',
            'networkWatcherId': 'network_watcher_id',
            'error': 'error',
            'flowAnalyticsConfiguration': 'flow_analytics_configuration',
            'properties': 'properties',
            'tags': 'tags',
            'targetResourceId': 'target_resource_id',
        },
    },
}


def GET(kind, params, cloud='azure'):
    spec = API_SPECS[kind]
    request_spec = spec['request']
    path = request_spec['path'].format(**params)
    host = request_spec.get(
        'host',
        {'azure': 'management.azure.com', 'usgov': 'management.usgovcloudapi.net'},
    )
    if type(host) is dict:
        host = host[cloud]
    host = host.format(**params)
    auth_aud = request_spec.get('auth_audience', host)
    api_version = request_spec.get('api-version')
    query_params = '?' + urlencode(
        updated(
            {'api-version': api_version} if api_version else {},
            request_spec.get('params'),
        )
    )
    bearer_token = CREDS[(CLIENT, TENANT, SECRET, auth_aud)].token['access_token']
    url = f'https://{host}{path}{query_params}'
    log.debug(f'GET {url}')
    result = requests.get(
        url,
        headers={
            'Authorization': 'Bearer ' + bearer_token,
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'x-ms-version': api_version,
        },
    )
    log.debug(f'<- {result.status_code}')
    response = json.loads(
        json_dumps(xmltodict.parse(result.text.encode()))
        if result.text.startswith('<?xml')
        else result.text
    )

    # empty lists of values are recorded as empty rows
    # error values are recorded as rows with error and empty value cols
    # normal values are recorded with populated values and an empty error col
    def response_values(response):
        for vk in spec.get('response_value_key', 'value').split('.'):
            if response is None or vk not in response:
                break
            response = response[vk]

        return (
            response
            if type(response) is list
            else [response]
            if type(response) is dict
            else [{'error': response}]
        ) or [{}]

    values = [
        updated(
            {},
            v,
            params,
            headerDate=parse_date(result.headers['Date']),
            tenantId=TENANT,
        )
        for v in response_values(response)
    ]

    if 'nextLink' in result:
        log.info(f"nextLink {result['nextLink']}, len(data)={len(values)}")

    return [{spec['response'][k]: v for k, v in x.items()} for x in values]


def ingest(table_name, options, run_now=False, dryrun=False):
    global CLIENT
    global TENANT
    global SECRET

    connection_name = options['name']

    for cred in options['credentials']:
        CLIENT = cred['client']
        TENANT = cred['tenant']
        SECRET = cred['secret']

        cloud = (
            cred['cloud']
            if 'cloud' in cred
            else ('usgov' if cred.get('gov') else 'azure')
        )
        table_name_part = '' if connection_name == 'default' else f'_{connection_name}'
        table_prefix = f'data.azure_collect{table_name_part}'

        def load_table(kind, **params):
            values = GET(kind, params, cloud=cloud)
            kind = 'connection' if kind == 'subscriptions' else kind
            db.insert(f'{table_prefix}_{kind}', values, dryrun=dryrun)
            return values

        for s in load_table('subscriptions'):
            sid = s.get('subscription_id')
            if sid is None:
                log.debug(f'subscription without id: {s}')
                continue

            load_table('virtual_machines', subscriptionId=sid)
            load_table('users', subscriptionId=sid)
            load_table('groups', subscriptionId=sid)

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
                    load_table('vaults_keys', subscriptionId=sid, vaultName=v['name'])
                    load_table(
                        'vaults_secrets', subscriptionId=sid, vaultName=v['name']
                    )

        load_table('service_principals')
        load_table('reports_credential_user_registration_details')


def main(table_name, tenant, client, secret, cloud, dryrun):
    ingest(
        table_name,
        {
            'name': 'default',
            'credentials': [
                {'tenant': tenant, 'client': client, 'secret': secret, 'cloud': cloud}
            ],
        },
        dryrun=dryrun,
    )


if __name__ == '__main__':
    fire.Fire(main)
