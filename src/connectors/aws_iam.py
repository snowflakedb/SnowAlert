from botocore.exceptions import ClientError
from dateutil.parser import parse as parse_date
import fire
from multiprocessing import Pool
from typing import Dict, List, Generator

from connectors.utils import sts_assume_role
from runners.helpers import db, log


AUDIT_ASSUMER = ''
MASTER_READER = ''
READER_EIDS = ''
AUDIT_READER_ROLE = 'audit-reader'

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'audit_assumer',
        'title': "Audit Assumer ARN",
        'prompt': "The role that does the assuming in all the org's accounts",
        'placeholder': "arn:aws:iam::1234567890987:role/audit-assumer",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'master_reader',
        'title': "The reader role on Org's master account",
        'prompt': "Role to be assumed for auditing the master account",
        'placeholder': "arn:aws:iam::987654321012:role/audit-reader",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'audit_reader',
        'title': "The reader role in Org's accounts",
        'prompt': "Role to be assumed for auditing the other accounts",
        'placeholder': "audit-reader",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'reader_eids',
        'title': "Reader EIDs",
        'prompt': "External Id's on the roles that need assuming",
        'secret': True,
    },
]

KEYS_TO_COLUMNS = {
    'list_accounts': {
        'Id': 'id',
        'Arn': 'arn',
        'Email': 'email',
        'Name': 'name',
        'Status': 'status',
        'JoinedMethod': 'joined_method',
        'JoinedTimestamp': 'joined_timestamp',
    },
    'list_users': {
        'Arn': 'arn',
        'Path': 'path',
        'CreateDate': 'create_date',
        'UserId': 'user_id',
        'UserName': 'user_name',
        'PasswordLastUsed': 'password_last_used',
    },
    'list_groups_for_user': {
        'Arn': 'arn',
        'Path': 'path',
        'UserName': 'user_name',
        'CreateDate': 'create_date',
        'GroupId': 'group_id',
        'GroupName': 'group_name',
    },
    'list_policies': {
        'Arn': 'arn',
        'Path': 'path',
        'PolicyName': 'policy_name',
        'CreateDate': 'create_date',
        'UpdateDate': 'update_date',
        'AttachmentCount': 'attachment_count',
        'IsAttachable': 'is_attachable',
        'PolicyId': 'policy_id',
        'DefaultVersionId': 'default_version_id',
        'PermissionsBoundaryUsageCount': 'permissions_boundary_usage_count',
    },
    'list_access_keys': {
        'CreateDate': 'create_date',
        'UserName': 'user_name',
        'Status': 'status',
        'AccessKeyId': 'access_key_id',
    },
    'get_login_profile': {
        'UserName': 'user_name',
        'CreateDate': 'create_date',
        'PasswordResetRequired': 'password_reset_required',
    },
    'list_mfa_devices': {
        'UserName': 'user_name',
        'SerialNumber': 'serial_number',
        'EnableDate': 'enable_date',
    },
    'list_attached_user_policies': {
        'UserName': 'user_name',
        'PolicyName': 'policy_name',
        'PolicyArn': 'policy_arn',
    },
    'list_user_policies': {
        'AccountID': 'account_id',
        'UserName': 'user_name',
        'PolicyName': 'policy_name',
    },
    'get_account_password_policy': {
        'AllowUsersToChangePassword': 'allow_users_to_change_password',
        'RequireLowercaseCharacters': 'require_lowercase_characters',
        'RequireUppercaseCharacters': 'require_uppercase_characters',
        'MinimumPasswordLength': 'minimum_password_length',
        'MaxPasswordAge': 'max_password_age',
        'PasswordReusePrevention': 'password_reuse_prevention',
        'RequireNumbers': 'require_numbers',
        'RequireSymbols': 'require_symbols',
        'HardExpiry': 'hard_expiry',
        'ExpirePasswords': 'expire_passwords',
    },
    'get_account_summary': {
        'UsersQuota': 'users_quota',
        'GroupsPerUserQuota': 'groups_per_user_quota',
        'AttachedPoliciesPerGroupQuota': 'attached_policies_per_group_quota',
        'PoliciesQuota': 'policies_quota',
        'GroupsQuota': 'groups_quota',
        'InstanceProfiles': 'instance_profiles',
        'SigningCertificatesPerUserQuota': 'signing_certificates_per_user_quota',
        'PolicySizeQuota': 'policy_size_quota',
        'PolicyVersionsInUseQuota': 'policy_versions_in_use_quota',
        'RolePolicySizeQuota': 'role_policy_size_quota',
        'AccountSigningCertificatesPresent': 'account_signing_certificates_present',
        'Users': 'users',
        'ServerCertificatesQuota': 'server_certificates_quota',
        'ServerCertificates': 'server_certificates',
        'AssumeRolePolicySizeQuota': 'assume_role_policy_size_quota',
        'Groups': 'groups',
        'MFADevicesInUse': 'mfa_devices_in_use',
        'RolesQuota': 'roles_quota',
        'VersionsPerPolicyQuota': 'versions_per_policy_quota',
        'AccountAccessKeysPresent': 'account_access_keys_present',
        'Roles': 'roles',
        'AccountMFAEnabled': 'account_mfa_enabled',
        'MFADevices': 'mfa_devices',
        'Policies': 'policies',
        'GroupPolicySizeQuota': 'group_policy_size_quota',
        'InstanceProfilesQuota': 'instance_profiles_quota',
        'AccessKeysPerUserQuota': 'access_keys_per_user_quota',
        'AttachedPoliciesPerRoleQuota': 'attached_policies_per_role_quota',
        'PolicyVersionsInUse': 'policy_versions_in_use',
        'Providers': 'providers',
        'AttachedPoliciesPerUserQuota': 'attached_policies_per_user_quota',
        'UserPolicySizeQuota': 'user_policy_size_quota',
        'GlobalEndpointTokenVersion': 'global_endpoint_token_version',
    },
    'list_entities_for_policy': {
        'PolicyArn': 'policy_arn',
        'GroupName': 'group_name',
        'GroupId': 'group_id',
        'UserName': 'user_name',
        'UserId': 'user_id',
        'RoleName': 'role_name',
        'RoleId': 'role_id',
    },
    'get_policy_version': {
        'PolicyArn': 'policy_arn',
        'VersionId': 'version_id',
        'CreateDate': 'create_date',
        'Document': 'document',
        'IsDefaultVersion': 'is_default_version',
    },
}


def updated(d, *ds, **kwargs):
    """Shallow merges dictionaries together, mutating + returning first arg"""
    for new_d in ds:
        d.update(new_d)
    if kwargs:
        d.update(kwargs)
    return d


def aws_collect(client, method, entity_name, params=None):
    if params is None:
        params = {}

    try:
        pages = (
            client.get_paginator(method).paginate(**params)
            if client.can_paginate(method)
            else [getattr(client, method)(**params)]
        )

    except client.exceptions.NoSuchEntityException as e:
        pages = [updated({entity: {}}, e.response)]

    for page in pages:
        entities = [entity_name] if type(entity_name) is str else entity_name
        for entity in entities:
            ents = page[entity]
            ent_iterator = [ents] if type(ents) is dict else ents
            for ent in ent_iterator:
                # e.g. {"PolicyNames": ["name1", "name2", ...]
                if type(ent) is str and entity.endswith('s'):
                    ent = {entity[:-1]: ent}
                ent['ResponseHeaderDate'] = parse_date(
                    page['ResponseMetadata']['HTTPHeaders']['date']
                )
                yield ent


def load_aws_iam(from_account_with_id) -> Generator[Dict[str, List[dict]], None, None]:
    account_arn = f'arn:aws:iam::{from_account_with_id}:role/{AUDIT_READER_ROLE}'

    try:
        session = sts_assume_role(
            src_role_arn=AUDIT_ASSUMER,
            dest_role_arn=account_arn,
            dest_external_id=READER_EIDS,
        )

    except ClientError as e:
        log.error(e)
        yield {}
        return

    iam = session.client('iam')

    account_info = {'AccountId': from_account_with_id}

    yield {
        'get_account_summary': [
            updated(u, account_info)
            for u in aws_collect(iam, 'get_account_summary', 'SummaryMap')
        ]
    }

    yield {
        'get_account_password_policy': [
            updated(u, account_info)
            for u in aws_collect(iam, 'get_account_password_policy', 'PasswordPolicy')
        ]
    }

    users = [updated(u, account_info) for u in aws_collect(iam, 'list_users', 'Users')]
    yield {'list_users': users}

    yield {
        'list_groups_for_user': [
            updated(group, account_info, {'UserName': user['UserName']})
            for user in users
            for group in aws_collect(
                iam, 'list_groups_for_user', 'Groups', {'UserName': user['UserName']}
            )
        ]
    }

    yield {
        'list_access_keys': [
            updated(access_key, account_info, {'UserName': user['UserName']})
            for user in users
            for access_key in aws_collect(
                iam,
                'list_access_keys',
                'AccessKeyMetadata',
                {'UserName': user['UserName']},
            )
        ]
    }

    yield {
        'get_login_profile': [
            updated(login_profile, account_info, {'UserName': user['UserName']})
            for user in users
            for login_profile in aws_collect(
                iam, 'get_login_profile', 'LoginProfile', {'UserName': user['UserName']}
            )
        ]
    }

    yield {
        'list_mfa_devices': [
            updated(mfa_device, account_info, {'UserName': user['UserName']})
            for user in users
            for mfa_device in aws_collect(
                iam, 'list_mfa_devices', 'MFADevices', {'UserName': user['UserName']}
            )
        ]
    }

    yield {
        'list_attached_user_policies': [
            updated(user_policy, account_info, {'UserName': user['UserName']})
            for user in users
            for user_policy in aws_collect(
                iam,
                'list_attached_user_policies',
                'AttachedPolicies',
                {'UserName': user['UserName']},
            )
        ]
    }

    yield {
        'list_user_policies': [
            updated(user_policy, account_info, {'UserName': user['UserName']})
            for user in users
            for user_policy in aws_collect(
                iam, 'list_user_policies', 'PolicyNames', {'UserName': user['UserName']}
            )
        ]
    }

    policies = [
        updated(u, account_info) for u in aws_collect(iam, 'list_policies', 'Policies')
    ]
    yield {'list_policies': policies}

    yield {
        'get_policy_version': [
            updated(version, account_info, {'PolicyArn': policy['Arn']})
            for policy in policies
            for version in aws_collect(
                iam,
                'get_policy_version',
                'PolicyVersion',
                {'PolicyArn': policy['Arn'], 'VersionId': policy['DefaultVersionId']},
            )
        ]
    }

    yield {
        'list_entities_for_policy': [
            updated(entity, account_info, {'PolicyArn': policy['Arn']})
            for policy in policies
            for entity in aws_collect(
                iam,
                'list_entities_for_policy',
                ['PolicyGroups', 'PolicyUsers', 'PolicyRoles'],
                {'PolicyArn': policy['Arn']},
            )
        ]
    }


def insert_list(name, values, table_name=None):
    table_name = table_name or f'data.aws_iam_{name}'
    k2c = {'AccountId': 'account_id', 'ResponseHeaderDate': 'recorded_at'}
    k2c.update(KEYS_TO_COLUMNS[name])
    rows = [{k2c[k]: v for k, v in value.items()} for value in values]
    log.info(f'inserting {len(rows)} rows into {table_name}')
    return db.insert(table_name, rows)


def collect_aws_iam(from_account_with_id):
    return [
        updated(
            insert_list(name, values), list_name=name, account_id=from_account_with_id
        )
        for lists in load_aws_iam(from_account_with_id)
        for name, values in lists.items()
    ]


def ingest(table_name, options):
    global AUDIT_ASSUMER
    global MASTER_READER
    global READER_EIDS
    global AUDIT_READER_ROLE
    AUDIT_ASSUMER = options.get('audit_assumer', '')
    MASTER_READER = options.get('master_reader', '')
    READER_EIDS = options.get('reader_eids', '')
    AUDIT_READER_ROLE = options.get('audit_reader_role', '')

    org_client = sts_assume_role(
        src_role_arn=AUDIT_ASSUMER,
        dest_role_arn=MASTER_READER,
        dest_external_id=READER_EIDS,
    ).client('organizations')

    accounts = [a for a in aws_collect(org_client, 'list_accounts', 'Accounts')]
    retval = [insert_list('list_accounts', accounts, table_name=f'data.{table_name}')]
    if options.get('collect_aws_iam') == 'all':
        retval += Pool(100).map(collect_aws_iam, [a['Id'] for a in accounts])
    return retval


def main(audit_assumer, master_reader, reader_eids, audit_reader_role):
    print(ingest({
        'audit_assumer': audit_assumer,
        'master_reader': master_reader,
        'reader_eids': reader_eids,
        'audit_reader_role': audit_reader_role
    }))


if __name__ == '__main__':
    fire.Fire(main)
