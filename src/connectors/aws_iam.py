from botocore.exceptions import ClientError
from collections import defaultdict
from dateutil.parser import parse as parse_date
import fire
from typing import Dict, List, Generator

from connectors.utils import sts_assume_role, qmap
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

AWS_API_METHODS = {
    'list_accounts': {
        'response': {
            'Accounts': {
                'Id': 'id',
                'Arn': 'arn',
                'Email': 'email',
                'Name': 'name',
                'Status': 'status',
                'JoinedMethod': 'joined_method',
                'JoinedTimestamp': 'joined_timestamp',
            }
        }
    },
    'list_users': {
        'response': {
            'Users': {
                'Arn': 'arn',
                'Path': 'path',
                'CreateDate': 'create_date',
                'UserId': 'user_id',
                'UserName': 'user_name',
                'PasswordLastUsed': 'password_last_used',
            }
        }
    },
    'list_groups_for_user': {
        'response': {
            'Groups': {
                'Arn': 'arn',
                'Path': 'path',
                'UserName': 'user_name',
                'CreateDate': 'create_date',
                'GroupId': 'group_id',
                'GroupName': 'group_name',
            }
        }
    },
    'list_policies': {
        'response': {
            'Policies': {
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
            }
        }
    },
    'list_access_keys': {
        'response': {
            'AccessKeyMetadata': {
                'CreateDate': 'create_date',
                'UserName': 'user_name',
                'Status': 'status',
                'AccessKeyId': 'access_key_id',
            }
        }
    },
    'get_login_profile': {
        'response': {
            'LoginProfile': {
                'UserName': 'user_name',
                'CreateDate': 'create_date',
                'PasswordResetRequired': 'password_reset_required',
            }
        }
    },
    'list_mfa_devices': {
        'response': {
            'MFADevices': {
                'UserName': 'user_name',
                'SerialNumber': 'serial_number',
                'EnableDate': 'enable_date',
            }
        }
    },
    'list_attached_user_policies': {
        'response': {
            'AttachedPolicies': {
                'UserName': 'user_name',
                'PolicyName': 'policy_name',
                'PolicyArn': 'policy_arn',
            }
        }
    },
    'list_user_policies': {
        'response': {
            'PolicyNames': {
                'AccountID': 'account_id',
                'UserName': 'user_name',
                'PolicyName': 'policy_name',
            }
        }
    },
    'get_account_password_policy': {
        'response': {
            'PasswordPolicy': {
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
            }
        }
    },
    'get_account_summary': {
        'response': {
            'SummaryMap': {
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
            }
        }
    },
    'list_entities_for_policy': {
        'response': {
            'PolicyGroups': {
                'PolicyArn': 'policy_arn',
                'GroupName': 'group_name',
                'GroupId': 'group_id',
            },
            'PolicyUsers': {
                'PolicyArn': 'policy_arn',
                'UserName': 'user_name',
                'UserId': 'user_id',
            },
            'PolicyRoles': {
                'PolicyArn': 'policy_arn',
                'RoleName': 'role_name',
                'RoleId': 'role_id',
            },
        }
    },
    'get_policy_version': {
        'response': {
            'PolicyVersion': {
                'PolicyArn': 'policy_arn',
                'VersionId': 'version_id',
                'CreateDate': 'create_date',
                'Document': 'document',
                'IsDefaultVersion': 'is_default_version',
            }
        }
    },
}


def updated(d, *ds, **kwargs):
    """Shallow merges dictionaries together, mutating + returning first arg"""
    for new_d in ds:
        d.update(new_d)
    if kwargs:
        d.update(kwargs)
    return d


def aws_collect(client, method, params=None):
    if params is None:
        params = {}

    k2c = AWS_API_METHODS[method]['response']
    ent_keys = k2c.keys()  # we'll be expecting response to have these keys

    try:
        pages = (
            client.get_paginator(method).paginate(**params)
            if client.can_paginate(method)
            else [getattr(client, method)(**params)]
        )

    except client.exceptions.NoSuchEntityException as e:
        pages = [updated({ent: {} for ent in ent_keys}, e.response)]

    for page in pages:
        for ent_key in ent_keys:
            ents = page[ent_key]

            # treat singular entities from get_* like list with one ent.
            ents = [ents] if type(ents) is dict else ents

            for ent in ents:
                # ents = {"PolicyNames": ["p1"]} -> [{"PolicyName": "p1"}]
                if type(ent) is str and ent_key.endswith('s'):
                    ent = {ent_key[:-1]: ent}

                ent['recorded_at'] = parse_date(
                    page['ResponseMetadata']['HTTPHeaders']['date']
                )

                yield {v: ent.get(k) for k, v in k2c[ent_key].items()}


def load_aws_iam(
    account_id, method, params, add_task
) -> Generator[Dict[str, List[dict]], None, None]:
    account_arn = f'arn:aws:iam::{account_id}:role/{AUDIT_READER_ROLE}'
    account_info = {'account_id': account_id}

    try:
        session = sts_assume_role(
            src_role_arn=AUDIT_ASSUMER,
            dest_role_arn=account_arn,
            dest_external_id=READER_EIDS,
        )

    except ClientError as e:
        # record missing auditor role as empty account summary
        yield {
            method: [
                updated(
                    account_info,
                    recorded_at=parse_date(
                        e.response['ResponseMetadata']['HTTPHeaders']['date']
                    ),
                )
            ]
        }
        return

    iam = session.client('iam')

    if method == 'get_account_summary':
        yield {
            'get_account_summary': [
                updated(u, account_info)
                for u in aws_collect(iam, 'get_account_summary')
            ]
        }

    if method == 'get_account_password_policy':
        yield {
            'get_account_password_policy': [
                updated(u, account_info)
                for u in aws_collect(iam, 'get_account_password_policy')
            ]
        }

    if method == 'list_users':
        users = [updated(u, account_info) for u in aws_collect(iam, 'list_users')]
        yield {'list_users': users}
        for user in users:
            add_task(
                {
                    'account_id': account_id,
                    'methods': [
                        'list_groups_for_user',
                        'list_access_keys',
                        'get_login_profile',
                        'list_mfa_devices',
                        'list_user_policies',
                        'list_attached_user_policies',
                    ],
                    'params': {'UserName': user['user_name']},
                }
            )

    if method == 'list_groups_for_user':
        yield {
            'list_groups_for_user': [
                updated(group, account_info, {'user_name': params['UserName']})
                for group in aws_collect(iam, 'list_groups_for_user', params)
            ]
        }

    if method == 'list_access_keys':
        yield {
            'list_access_keys': [
                updated(access_key, account_info, {'user_name': params['UserName']})
                for access_key in aws_collect(iam, 'list_access_keys', params)
            ]
        }

    if method == 'get_login_profile':
        yield {
            'get_login_profile': [
                updated(login_profile, account_info, {'user_name': params['UserName']})
                for login_profile in aws_collect(iam, 'get_login_profile', params)
            ]
        }

    if method == 'list_mfa_devices':
        yield {
            'list_mfa_devices': [
                updated(mfa_device, account_info, {'user_name': params['UserName']})
                for mfa_device in aws_collect(iam, 'list_mfa_devices', params)
            ]
        }

    if method == 'list_attached_user_policies':
        yield {
            'list_attached_user_policies': [
                updated(user_policy, account_info, {'user_name': params['UserName']})
                for user_policy in aws_collect(
                    iam, 'list_attached_user_policies', params
                )
            ]
        }

    if method == 'list_user_policies':
        yield {
            'list_user_policies': [
                updated(user_policy, account_info, {'user_name': params['UserName']})
                for user_policy in aws_collect(iam, 'list_user_policies', params)
            ]
        }

    if method == 'list_policies':
        policies = [updated(u, account_info) for u in aws_collect(iam, 'list_policies')]
        yield {'list_policies': policies}
        for policy in policies:
            add_task(
                {
                    'account_id': account_id,
                    'method': 'get_policy_version',
                    'params': {
                        'PolicyArn': policy['arn'],
                        'VersionId': policy['default_version_id'],
                    },
                }
            )
            add_task(
                {
                    'account_id': account_id,
                    'method': 'list_entities_for_policy',
                    'params': {'PolicyArn': policy['arn']},
                }
            )
    if method == 'get_policy_version':
        yield {
            'get_policy_version': [
                updated(version, account_info, {'policy_arn': params['PolicyArn']})
                for version in aws_collect(iam, 'get_policy_version', params)
            ]
        }

    if method == 'list_entities_for_policy':
        yield {
            'list_entities_for_policy': [
                updated(entity, account_info, {'policy_arn': params['PolicyArn']})
                for entity in aws_collect(iam, 'list_entities_for_policy', params)
            ]
        }


def insert_list(name, values, table_name=None):
    table_name = table_name or f'data.aws_iam_{name}'
    log.info(f'inserting {len(values)} values into {table_name}')
    return db.insert(table_name, values)


def collect_aws_iam(task, add_task=None):
    log.info(f'processing {task}')
    account_id = task['account_id']
    methods = task.get('methods')
    methods = [task['method']] if methods is None else methods
    params = task.get('params', {})

    for method in methods:
        yield from load_aws_iam(account_id, method, params, add_task)


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

    accounts = [a for a in aws_collect(org_client, 'list_accounts')]
    insert_list('list_accounts', accounts, table_name=f'data.{table_name}')
    if options.get('collect_aws_iam') == 'all':
        results = defaultdict(list)
        lists = qmap(
            50,
            collect_aws_iam,
            [
                {'method': method, 'account_id': a['id']}
                for a in accounts
                for method in [
                    'get_account_summary',
                    'get_account_password_policy',
                    'list_users',
                    'list_policies',
                ]
            ],
        )
        for vs in lists:
            for k, xs in vs.items():
                results[k] += xs
        for k, xs in results.items():
            print(k, len(xs))
        for k, xs in results.items():
            insert_list(k, xs)


def main(table_name, audit_assumer, master_reader, reader_eids, audit_reader_role):
    print(
        ingest(
            table_name,
            {
                'audit_assumer': audit_assumer,
                'master_reader': master_reader,
                'reader_eids': reader_eids,
                'audit_reader_role': audit_reader_role,
            },
        )
    )


if __name__ == '__main__':
    fire.Fire(main)
