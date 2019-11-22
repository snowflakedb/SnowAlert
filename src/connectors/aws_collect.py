"""AWS Collect
Load Inventory and Configuration of all accounts in your Org using auditor Roles
"""

from botocore.exceptions import ClientError
import csv
from dateutil.parser import parse as parse_date
import fire
import io
from typing import Dict, List, Generator

from runners.helpers.dbconfig import ROLE as SA_ROLE

from connectors.utils import sts_assume_role, qmap_mp, updated, yaml_dump
from runners.helpers import db, log
from runners.utils import groups_of


AUDIT_ASSUMER_ARN = 'arn:aws:iam::1234567890987:role/audit-assumer'
MASTER_READER_ARN = 'arn:aws:iam::987654321012:role/audit-reader'
AUDIT_READER_ROLE = 'audit-reader'
READER_EID = ''

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'audit_assumer_arn',
        'title': "Audit Assumer ARN",
        'prompt': "The role that does the assuming in all the org's accounts",
        'placeholder': "arn:aws:iam::1234567890987:role/audit-assumer",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'master_reader_arn',
        'title': "The reader role on Org's master account",
        'prompt': "Role to be assumed for auditing the master account",
        'placeholder': "arn:aws:iam::987654321012:role/audit-reader",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'audit_reader_role',
        'title': "The reader role in Org's accounts",
        'prompt': "Role to be assumed for auditing the other accounts",
        'placeholder': "audit-reader",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'reader_eid',
        'title': "Reader EID",
        'prompt': "External Id on the roles that need assuming",
        'secret': True,
    },
]

AWS_API_METHODS = {
    'organizations.list_accounts': {
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
    'ec2.describe_instances': {
        'response': {
            'Reservations': {
                'Groups': 'groups',
                'Instances': 'instances',
                'OwnerId': 'owner_id',
                'ReservationId': 'reservation_id',
            }
        }
    },
    'ec2.describe_security_groups': {
        'response': {
            'SecurityGroups': {
                'Description': 'description',
                'GroupName': 'group_name',
                'IpPermissions': 'ip_permissions',
                'OwnerId': 'owner_id',
                'GroupId': 'group_id',
                'IpPermissionsEgress': 'ip_permissions_egress',
                'Tags': 'tags',
                'VpcId': 'vpc_id',
            }
        }
    },
    'config.describe_configuration_recorders': {
        'response': {
            'ConfigurationRecorders': {
                'name': 'name',
                'roleARN': 'role_arn',
                'recordingGroup': 'recording_group',
            }
        }
    },
    'kms.list_keys': {
        'response': {'Keys': {'KeyId': 'key_id', 'KeyArn': 'key_arn'}},
        'children': [
            {'method': 'kms.get_key_rotation_status', 'params': {'KeyId': 'key_id'}}
        ],
    },
    'kms.get_key_rotation_status': {
        'response': {'KeyRotationEnabled': 'key_rotation_enabled'}
    },
    'cloudtrail.get_event_selectors': {
        'response': {
            'TrailARN': 'trail_arn',
            'EventSelectors': {
                'ReadWriteType': 'read_write_type',
                'IncludeManagementEvents': 'include_management_events',
                'DataResources': 'data_resources',
            },
        }
    },
    'cloudtrail.describe_trails': {
        'response': {
            'trailList': {
                'Name': 'name',
                'S3BucketName': 's3_bucket_name',
                'S3KeyPrefix': 's3_key_prefix',
                'SnsTopicName': 'sns_topic_name',
                'SnsTopicARN': 'sns_topic_arn',
                'IncludeGlobalServiceEvents': 'include_global_service_events',
                'IsMultiRegionTrail': 'is_multi_region_trail',
                'HomeRegion': 'home_region',
                'TrailARN': 'trail_arn',
                'LogFileValidationEnabled': 'log_file_validation_enabled',
                'CloudWatchLogsLogGroupArn': 'cloud_watch_logs_log_group_arn',
                'CloudWatchLogsRoleArn': 'cloud_watch_logs_role_arn',
                'KmsKeyId': 'kms_key_id',
                'HasCustomEventSelectors': 'has_custom_event_selectors',
                'IsOrganizationTrail': 'is_organization_trail',
            }
        },
        'children': [
            {'method': 'cloudtrail.get_trail_status', 'params': {'Name': 'trail_arn'}},
            {
                'method': 'cloudtrail.get_event_selectors',
                'params': {'TrailName': 'trail_arn'},
            },
        ],
    },
    'cloudtrail.get_trail_status': {
        'response': {
            'IsLogging': 'is_logging',
            'LatestDeliveryError': 'latest_delivery_error',
            'LatestNotificationError': 'latest_notification_error',
            'LatestDeliveryTime': 'latest_delivery_time',
            'LatestNotificationTime': 'latest_notification_time',
            'StartLoggingTime': 'start_logging_time',
            'StopLoggingTime': 'stop_logging_time',
            'LatestCloudWatchLogsDeliveryError': 'latest_cloud_watch_logs_delivery_error',
            'LatestCloudWatchLogsDeliveryTime': 'latest_cloud_watch_logs_delivery_time',
            'LatestDigestDeliveryTime': 'latest_digest_delivery_time',
            'LatestDigestDeliveryError': 'latest_digest_delivery_error',
            'LatestDeliveryAttemptTime': 'latest_delivery_attempt_time',
            'LatestNotificationAttemptTime': 'latest_notification_attempt_time',
            'LatestNotificationAttemptSucceeded': 'latest_notification_attempt_succeeded',
            'LatestDeliveryAttemptSucceeded': 'latest_delivery_attempt_succeeded',
            'TimeLoggingStarted': 'time_logging_started',
            'TimeLoggingStopped': 'time_logging_stopped',
        }
    },
    'iam.list_users': {
        'response': {
            'Users': {
                'Arn': 'arn',
                'Path': 'path',
                'CreateDate': 'create_date',
                'UserId': 'user_id',
                'UserName': 'user_name',
                'PasswordLastUsed': 'password_last_used',
            }
        },
        'children': [
            {
                'methods': [
                    'iam.list_groups_for_user',
                    'iam.list_access_keys',
                    'iam.get_login_profile',
                    'iam.list_mfa_devices',
                    'iam.list_user_policies',
                    'iam.list_attached_user_policies',
                ],
                'params': {'UserName': 'user_name'},
            }
        ],
    },
    'iam.list_groups_for_user': {
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
    'iam.list_policies': {
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
        },
        'children': [
            {
                'method': 'iam.get_policy_version',
                'params': {'PolicyArn': 'arn', 'VersionId': 'default_version_id'},
            },
            {'method': 'iam.list_entities_for_policy', 'params': {'PolicyArn': 'arn'}},
        ],
    },
    'iam.list_access_keys': {
        'response': {
            'AccessKeyMetadata': {
                'CreateDate': 'create_date',
                'UserName': 'user_name',
                'Status': 'status',
                'AccessKeyId': 'access_key_id',
            }
        }
    },
    'iam.get_login_profile': {
        'response': {
            'LoginProfile': {
                'UserName': 'user_name',
                'CreateDate': 'create_date',
                'PasswordResetRequired': 'password_reset_required',
            }
        }
    },
    'iam.list_mfa_devices': {
        'response': {
            'MFADevices': {
                'UserName': 'user_name',
                'SerialNumber': 'serial_number',
                'EnableDate': 'enable_date',
            }
        }
    },
    'iam.list_attached_user_policies': {
        'response': {
            'AttachedPolicies': {
                'UserName': 'user_name',
                'PolicyName': 'policy_name',
                'PolicyArn': 'policy_arn',
            }
        }
    },
    'iam.list_user_policies': {
        'response': {
            'PolicyNames': {
                'AccountID': 'account_id',
                'UserName': 'user_name',
                'PolicyName': 'policy_name',
            }
        }
    },
    'iam.list_account_aliases': {
        'response': {'AccountAliases': {'AccountAliase': 'account_alias'}}
    },
    'iam.get_account_password_policy': {
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
    'iam.generate_credential_report': {
        'response': {'State': 'state', 'Description': 'description'}
    },
    'iam.get_credential_report': {
        'response': {
            'Content': ('csv', 'content'),
            'ReportFormat': 'report_format',
            'GeneratedTime': 'generated_time',
        }
    },
    'iam.list_virtual_mfa_devices': {
        'response': {
            'VirtualMFADevices': {
                'SerialNumber': 'serial_number',
                'Base32StringSeed': 'base32_string_seed',
                'QRCodePNG': 'qr_code_png',
                'User': 'user',
            }
        }
    },
    'iam.get_account_summary': {
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
    's3.list_buckets': {
        'response': {
            'Buckets': {'Name': 'bucket_name', 'CreationDate': 'bucket_creation_date'},
            'Owner': {'DisplayName': 'owner_display_name', 'ID': 'owner_id'},
        },
        'children': [
            {
                'methods': [
                    's3.get_bucket_acl',
                    's3.get_bucket_policy',
                    's3.get_bucket_logging',
                ],
                'params': {'Bucket': 'bucket_name'},
            }
        ],
    },
    's3.get_bucket_acl': {
        'response': {
            'Owner': {'DisplayName': 'owner_display_name', 'ID': 'owner_id'},
            'Grants': {'Grantee': 'grants_grantee', 'Permission': 'grants_permission'},
        }
    },
    's3.get_bucket_policy': {'response': {'Policy': 'policy'}},
    's3.get_bucket_logging': {
        'response': {
            'LoggingEnabled': {
                'TargetBucket': 'target_bucket',
                'TargetGrants': 'target_grants',
                'TargetPrefix': 'target_prefix',
            }
        }
    },
    'iam.list_entities_for_policy': {
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
    'iam.get_policy_version': {
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

LANDING_TABLE_COLUMNS = [
    ('recorded_at', 'TIMESTAMP_NTZ'),
    ('id', 'VARCHAR'),
    ('arn', 'VARCHAR'),
    ('email', 'VARCHAR'),
    ('name', 'VARCHAR'),
    ('status', 'VARCHAR'),
    ('joined_method', 'VARCHAR'),
    ('joined_timestamp', 'TIMESTAMP_NTZ'),
]

SUPPLEMENTARY_TABLES = {
    'iam_generate_credential_report': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('state', 'VARCHAR'),
        ('description', 'VARCHAR'),
    ],
    'iam_get_account_password_policy': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('allow_users_to_change_password', 'VARCHAR'),
        ('require_lowercase_characters', 'VARCHAR'),
        ('require_uppercase_characters', 'VARCHAR'),
        ('minimum_password_length', 'NUMBER'),
        ('password_reuse_prevention', 'NUMBER'),
        ('max_password_age', 'NUMBER'),
        ('require_numbers', 'VARCHAR'),
        ('require_symbols', 'VARCHAR'),
        ('hard_expiry', 'VARCHAR'),
        ('expire_passwords', 'VARCHAR'),
    ],
    'iam_list_access_keys': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('user_name', 'VARCHAR'),
        ('status', 'VARCHAR'),
        ('create_date', 'TIMESTAMP_NTZ'),
        ('access_key_id', 'VARCHAR'),
    ],
    'iam_get_account_summary': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('users_quota', 'NUMBER'),
        ('groups_per_user_quota', 'NUMBER'),
        ('attached_policies_per_group_quota', 'NUMBER'),
        ('policies_quota', 'NUMBER'),
        ('groups_quota', 'NUMBER'),
        ('instance_profiles', 'NUMBER'),
        ('signing_certificates_per_user_quota', 'NUMBER'),
        ('policy_size_quota', 'NUMBER'),
        ('policy_versions_in_use_quota', 'NUMBER'),
        ('role_policy_size_quota', 'NUMBER'),
        ('account_signing_certificates_present', 'NUMBER'),
        ('users', 'NUMBER'),
        ('server_certificates_quota', 'NUMBER'),
        ('server_certificates', 'NUMBER'),
        ('assume_role_policy_size_quota', 'NUMBER'),
        ('groups', 'NUMBER'),
        ('mfa_devices_in_use', 'NUMBER'),
        ('roles_quota', 'NUMBER'),
        ('versions_per_policy_quota', 'NUMBER'),
        ('account_access_keys_present', 'NUMBER'),
        ('roles', 'NUMBER'),
        ('account_mfa_enabled', 'NUMBER'),
        ('mfa_devices', 'NUMBER'),
        ('policies', 'NUMBER'),
        ('group_policy_size_quota', 'NUMBER'),
        ('instance_profiles_quota', 'NUMBER'),
        ('access_keys_per_user_quota', 'NUMBER'),
        ('attached_policies_per_role_quota', 'NUMBER'),
        ('policy_versions_in_use', 'NUMBER'),
        ('providers', 'NUMBER'),
        ('attached_policies_per_user_quota', 'NUMBER'),
        ('user_policy_size_quota', 'NUMBER'),
        ('global_endpoint_token_version', 'NUMBER'),
    ],
    'GRANTS': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('generated_time', 'VARCHAR'),
        ('report_format', 'VARCHAR'),
        ('content', 'VARCHAR'),
        ('content_csv_parsed', 'VARIANT'),
    ],
    'iam_get_login_profile': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('user_name', 'VARCHAR'),
        ('create_date', 'TIMESTAMP_NTZ'),
        ('password_reset_required', 'VARCHAR'),
    ],
    'iam_get_policy_version': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('policy_arn', 'VARCHAR'),
        ('version_id', 'VARCHAR'),
        ('create_date', 'TIMESTAMP_NTZ'),
        ('document', 'VARIANT'),
        ('is_default_version', 'VARCHAR'),
    ],
    'iam_list_access_keys': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('user_name', 'VARCHAR'),
        ('status', 'VARCHAR'),
        ('create_date', 'TIMESTAMP_NTZ'),
        ('access_key_id', 'VARCHAR'),
    ],
    'iam_list_attached_user_policies': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('user_name', 'VARCHAR'),
        ('policy_name', 'VARCHAR'),
        ('policy_arn', 'VARCHAR'),
    ],
    'iam_list_entities_for_policy': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('policy_arn', 'VARCHAR'),
        ('group_id', 'VARCHAR'),
        ('group_name', 'VARCHAR'),
        ('user_id', 'VARCHAR'),
        ('user_name', 'VARCHAR'),
        ('role_id', 'VARCHAR'),
        ('role_name', 'VARCHAR'),
    ],
    'iam_list_groups_for_user': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('user_name', 'VARCHAR'),
        ('path', 'VARCHAR'),
        ('create_date', 'TIMESTAMP_NTZ'),
        ('group_id', 'VARCHAR'),
        ('arn', 'VARCHAR'),
        ('group_name', 'VARCHAR'),
    ],
    'iam_list_mfa_devices': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('user_name', 'VARCHAR'),
        ('serial_number', 'VARCHAR'),
        ('enable_date', 'TIMESTAMP_NTZ'),
    ],
    'iam_list_policies': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('policy_name', 'VARCHAR'),
        ('create_date', 'TIMESTAMP_NTZ'),
        ('attachment_count', 'NUMBER'),
        ('is_attachable', 'VARCHAR'),
        ('policy_id', 'VARCHAR'),
        ('default_version_id', 'VARCHAR'),
        ('path', 'VARCHAR'),
        ('arn', 'VARCHAR'),
        ('update_date', 'TIMESTAMP_NTZ'),
        ('permissions_boundary_usage_count', 'NUMBER'),
    ],
    'iam_list_user_policies': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('user_name', 'VARCHAR'),
        ('policy_name', 'VARCHAR'),
    ],
    'iam_list_users': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('user_name', 'VARCHAR'),
        ('password_last_used', 'TIMESTAMP_NTZ'),
        ('create_date', 'TIMESTAMP_NTZ'),
        ('user_id', 'VARCHAR'),
        ('path', 'VARCHAR'),
        ('arn', 'VARCHAR'),
    ],
    'iam_list_virtual_mfa_devices': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('serial_number', 'VARCHAR'),
        ('base32_string_seed', 'VARCHAR'),
        ('qr_code_png', 'VARCHAR'),
        ('user', 'VARIANT'),
    ],
    's3_list_buckets': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('bucket_name', 'VARCHAR'),
        ('bucket_creation_date', 'VARCHAR'),
        ('owner_display_name', 'VARCHAR'),
        ('owner_id', 'VARCHAR'),
    ],
    's3_get_bucket_acl': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'VARCHAR'),
        ('bucket', 'VARCHAR'),
        ('owner', 'VARCHAR'),
        ('grants_grantee', 'VARIANT'),
        ('grants_permission', 'VARCHAR'),
        ('owner_display_name', 'VARCHAR'),
        ('owner_id', 'VARCHAR'),
    ],
    's3_get_bucket_policy': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'STRING'),
        ('bucket', 'STRING'),
        ('policy', 'STRING'),
    ],
    's3_get_bucket_logging': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'STRING'),
        ('bucket', 'STRING'),
        ('target_bucket', 'STRING'),
        ('target_grants', 'VARIANT'),
        ('target_prefix', 'STRING'),
    ],
    'ec2_describe_security_groups': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'STRING'),
        ('description', 'STRING'),
        ('group_name', 'STRING'),
        ('ip_permissions', 'VARIANT'),
        ('owner_id', 'STRING'),
        ('group_id', 'STRING'),
        ('ip_permissions_egress', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('vpc_id', 'STRING'),
    ],
    'cloudtrail_describe_trails': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'STRING'),
        ('name', 'STRING'),
        ('s3_bucket_name', 'STRING'),
        ('s3_key_prefix', 'STRING'),
        ('sns_topic_name', 'STRING'),
        ('sns_topic_arn', 'STRING'),
        ('include_global_service_events', 'BOOLEAN'),
        ('is_multi_region_trail', 'BOOLEAN'),
        ('home_region', 'STRING'),
        ('trail_arn', 'STRING'),
        ('log_file_validation_enabled', 'BOOLEAN'),
        ('cloud_watch_logs_log_group_arn', 'STRING'),
        ('cloud_watch_logs_role_arn', 'STRING'),
        ('kms_key_id', 'STRING'),
        ('has_custom_event_selectors', 'BOOLEAN'),
        ('is_organization_trail', 'BOOLEAN'),
    ],
    'cloudtrail_get_trail_status': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'STRING'),
        ('trail_arn', 'STRING'),
        ('is_logging', 'BOOLEAN'),
        ('latest_delivery_error', 'STRING'),
        ('latest_notification_error', 'STRING'),
        ('latest_delivery_time', 'TIMESTAMP'),
        ('latest_notification_time', 'TIMESTAMP'),
        ('start_logging_time', 'TIMESTAMP'),
        ('stop_logging_time', 'TIMESTAMP'),
        ('latest_cloud_watch_logs_delivery_error', 'STRING'),
        ('latest_cloud_watch_logs_delivery_time', 'TIMESTAMP'),
        ('latest_digest_delivery_time', 'TIMESTAMP'),
        ('latest_digest_delivery_error', 'STRING'),
        ('latest_delivery_attempt_time', 'STRING'),
        ('latest_notification_attempt_time', 'STRING'),
        ('latest_notification_attempt_succeeded', 'STRING'),
        ('latest_delivery_attempt_succeeded', 'STRING'),
        ('time_logging_started', 'STRING'),
        ('time_logging_stopped', 'STRING'),
    ],
    'cloudtrail_get_event_selectors': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'STRING'),
        ('trail_arn', 'STRING'),
        ('read_write_type', 'STRING'),
        ('include_management_events', 'BOOLEAN'),
        ('data_resources', 'VARIANT'),
    ],
    'kms_list_keys': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'STRING'),
        ('key_id', 'STRING'),
        ('key_arn', 'STRING'),
    ],
    'kms_get_key_rotation_status': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'STRING'),
        ('key_id', 'STRING'),
        ('key_rotation_enabled', 'BOOLEAN'),
    ],
    'config_describe_configuration_recorders': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'STRING'),
        ('name', 'STRING'),
        ('role_arn', 'STRING'),
        ('recording_group', 'VARIANT'),
    ],
    'ec2_describe_instances': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'STRING'),
        ('groups', 'VARIANT'),
        ('instances', 'VARIANT'),
        ('owner_id', 'STRING'),
        ('reservation_id', 'STRING'),
    ],
    'iam_list_account_aliases': [
        ('recorded_at', 'TIMESTAMP_NTZ'),
        ('account_id', 'STRING'),
        ('account_alias', 'STRING'),
    ],
}


def connect(connection_name, options):
    table_prefix = f'aws_collect' + (
        '' if connection_name in ('', 'default') else connection_name
    )
    table_name = f'{table_prefix}_organizations_list_accounts_connection'
    landing_table = f'data.{table_name}'

    audit_assumer_arn = options['audit_assumer_arn']
    master_reader_arn = options['master_reader_arn']
    audit_reader_role = options['audit_reader_role']
    reader_eid = options['reader_eid']

    comment = yaml_dump(
        module='aws_collect',
        audit_assumer_arn=audit_assumer_arn,
        master_reader_arn=master_reader_arn,
        audit_reader_role=audit_reader_role,
        reader_eid=reader_eid,
        collect_apis='all'
    )

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')

    for table_postfix, cols in SUPPLEMENTARY_TABLES.items():
        supp_table = f'data.{table_prefix}_{table_postfix}'
        db.create_table(name=supp_table, cols=cols)
        db.execute(f'GRANT INSERT, SELECT ON {supp_table} TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': "AWS Collect connector tables created.",
    }


def aws_collect(client, method, params=None):
    if params is None:
        params = {}

    k2c = AWS_API_METHODS[method]['response']
    ent_keys = k2c.keys()  # we'll be expecting response to have these keys

    client_name, method_name = method.split('.', 1)

    pages = (
        client.get_paginator(method_name).paginate(**params)
        if client.can_paginate(method_name)
        else None
    )

    NotFoundExceptions = tuple(
        filter(
            None,
            [
                ClientError,
                getattr(client.exceptions, 'AccessDeniedException', None),
                getattr(client.exceptions, 'NoSuchEntityException', None),
            ],
        )
    )

    # this is a bit crufty, may not be necessary now that the for loop below
    # handles string values as {ent_key: cols} & {ent_key: ents}
    inline_object_response = all(type(v) in (str, tuple) for k, v in k2c.items())

    try:
        if pages is None:
            pages = [getattr(client, method_name)(**params)]
    except NotFoundExceptions as e:
        blank = None if inline_object_response else {}
        pages = [updated({ent: blank for ent in ent_keys}, e.response)]

    for page in pages:
        if inline_object_response:
            # e.g. *-credential-report methods
            def to_str(x):
                return x.decode() if type(x) is bytes else x

            parsers = {
                'csv': lambda v: [dict(x) for x in csv.DictReader(io.StringIO(v))]
            }

            yield updated(
                {v: to_str(page.get(k)) for k, v in k2c.items() if type(v) is str},
                {
                    f'{t[1]}_{t[0]}_parsed': parsers[t[0]](to_str(page.get(k)))
                    for k, t in k2c.items()
                    if type(t) is tuple
                },
                {t[1]: to_str(page.get(k)) for k, t in k2c.items() if type(t) is tuple},
                recorded_at=parse_date(page['ResponseMetadata']['HTTPHeaders']['date']),
            )
            continue

        for ent_key in ent_keys:
            ents = page.get(ent_key, {})
            cols = k2c[ent_key]
            cols = cols if type(cols) is dict else {ent_key: cols}
            cols = updated({'ResponseHeaderDate': 'recorded_at'}, cols)

            # treat singular entities from get_* like list with one ent.
            ents = [ents] if type(ents) is dict else ents
            ents = [{ent_key: ents}] if type(ents) is str else ents

            for ent in ents:
                # ents = {"PolicyNames": ["p1"]} -> [{"PolicyName": "p1"}]
                if type(ent) is str and ent_key.endswith('s'):
                    ent = {ent_key[:-1]: ent}

                ent['ResponseHeaderDate'] = parse_date(
                    page['ResponseMetadata']['HTTPHeaders']['date']
                )

                yield {v: ent.get(k) for k, v in cols.items()}


def load_aws_iam(
    account_id, method, params, add_task
) -> Generator[Dict[str, List[dict]], None, None]:
    account_arn = f'arn:aws:iam::{account_id}:role/{AUDIT_READER_ROLE}'
    account_info = {'account_id': account_id}

    client_name, method = method.split('.', 1)

    try:
        session = sts_assume_role(
            src_role_arn=AUDIT_ASSUMER_ARN,
            dest_role_arn=account_arn,
            dest_external_id=READER_EID,
        )

    except ClientError as e:
        # record missing auditor role as empty account summary
        yield {
            f'{client_name}.{method}': [
                updated(
                    account_info,
                    recorded_at=parse_date(
                        e.response['ResponseMetadata']['HTTPHeaders']['date']
                    ),
                )
            ]
        }
        return

    client = session.client(client_name)

    if method == 'list_virtual_mfa_devices':
        virtual_mfa_devices = [
            updated(u, account_info)
            for u in aws_collect(client, 'iam.list_virtual_mfa_devices')
        ]
        yield {'iam.list_virtual_mfa_devices': virtual_mfa_devices}

    if method == 'list_account_aliases':
        account_aliases = [
            updated(u, account_info)
            for u in aws_collect(client, 'iam.list_account_aliases')
        ]
        yield {'iam.list_account_aliases': account_aliases}

    if method == 'describe_instances':
        reservations = [
            updated(u, account_info)
            for u in aws_collect(client, 'ec2.describe_instances')
        ]
        yield {'ec2.describe_instances': reservations}

    if method == 'describe_configuration_recorders':
        config_recorders = [
            updated(u, account_info)
            for u in aws_collect(client, 'config.describe_configuration_recorders')
        ]
        yield {'config.describe_configuration_recorders': config_recorders}

    if method == 'describe_security_groups':
        security_groups = [
            updated(u, account_info)
            for u in aws_collect(client, 'ec2.describe_security_groups')
        ]
        yield {'ec2.describe_security_groups': security_groups}

    if method == 'list_keys':
        keys = [updated(u, account_info) for u in aws_collect(client, 'kms.list_keys')]
        yield {'kms.list_keys': keys}
        for key in keys:
            params = {'KeyId': key['key_id']}
            yield {
                'kms.get_key_rotation_status': [
                    updated(u, account_info, {'key_id': params['KeyId']})
                    for u in aws_collect(client, 'kms.get_key_rotation_status', params)
                ]
            }

    if method == 'describe_trails':
        trails = [
            updated(u, account_info)
            for u in aws_collect(client, 'cloudtrail.describe_trails')
        ]
        yield {'cloudtrail.describe_trails': trails}
        for trail in trails:
            params = {'Name': trail['trail_arn']}
            yield {
                'cloudtrail.get_trail_status': [
                    updated(u, account_info, {'trail_arn': params['Name']})
                    for u in aws_collect(client, 'cloudtrail.get_trail_status', params)
                ]
            }
            params = {'TrailName': trail['trail_arn']}
            yield {
                'cloudtrail.get_event_selectors': [
                    updated(u, account_info, {'trail_arn': params['TrailName']})
                    for u in aws_collect(
                        client, 'cloudtrail.get_event_selectors', params
                    )
                ]
            }

    if method == 'get_trail_status':
        yield {
            'cloudtrail.get_trail_status': [
                updated(u, account_info, {'name': params['Name']})
                for u in aws_collect(client, 'cloudtrail.get_trail_status', params)
            ]
        }

    if method == 'get_event_selectors':
        yield {
            'cloudtrail.get_event_selectors': [
                updated(u, account_info, {'name': params['TrailName']})
                for u in aws_collect(client, 'cloudtrail.get_event_selectors', params)
            ]
        }

    if method == 'list_buckets':
        buckets = [
            updated(u, account_info) for u in aws_collect(client, 's3.list_buckets')
        ]
        yield {'s3.list_buckets': buckets}
        for bucket_group in groups_of(1000, buckets):
            add_task(
                {
                    'account_id': account_id,
                    'methods': [
                        's3.get_bucket_acl',
                        's3.get_bucket_policy',
                        's3.get_bucket_logging',
                    ],
                    'params': [
                        {'Bucket': bucket['bucket_name']}
                        for bucket in bucket_group
                        if 'bucket_name' in bucket
                    ],
                }
            )

    if method == 'get_bucket_policy':
        yield {
            's3.get_bucket_policy': [
                updated(u, account_info, {'bucket': params['Bucket']})
                for u in aws_collect(client, 's3.get_bucket_policy', params)
            ]
        }

    if method == 'get_bucket_acl':
        yield {
            's3.get_bucket_acl': [
                updated(u, account_info, {'bucket': params['Bucket']})
                for u in aws_collect(client, 's3.get_bucket_acl', params)
            ]
        }

    if method == 'get_bucket_logging':
        yield {
            's3.get_bucket_logging': [
                updated(u, account_info, {'bucket': params['Bucket']})
                for u in aws_collect(client, 's3.get_bucket_logging', params)
            ]
        }

    if method == 'get_account_password_policy':
        yield {
            'iam.get_account_password_policy': [
                updated(u, account_info)
                for u in aws_collect(client, 'iam.get_account_password_policy')
            ]
        }

    if method == 'generate_credential_report':
        yield {
            'iam.generate_credential_report': [
                updated(u, account_info)
                for u in aws_collect(client, 'iam.generate_credential_report')
            ]
        }
        add_task({'account_id': account_id, 'methods': 'iam.get_credential_report'})

    if method == 'get_credential_report':
        yield {
            'iam.get_credential_report': [
                updated(u, account_info)
                for u in aws_collect(client, 'iam.get_credential_report')
            ]
        }

    if method == 'list_users':
        users = [
            updated(u, account_info) for u in aws_collect(client, 'iam.list_users')
        ]
        yield {'iam.list_users': users}
        for user_group in groups_of(1000, users):
            add_task(
                {
                    'account_id': account_id,
                    'methods': [
                        'iam.list_groups_for_user',
                        'iam.list_access_keys',
                        'iam.get_login_profile',
                        'iam.list_mfa_devices',
                        'iam.list_user_policies',
                        'iam.list_attached_user_policies',
                    ],
                    'params': [{'UserName': user['user_name']} for user in user_group],
                }
            )

    if method == 'list_groups_for_user':
        yield {
            'iam.list_groups_for_user': [
                updated(group, account_info, {'user_name': params['UserName']})
                for group in aws_collect(client, 'iam.list_groups_for_user', params)
            ]
        }

    if method == 'list_access_keys':
        yield {
            'iam.list_access_keys': [
                updated(access_key, account_info, {'user_name': params['UserName']})
                for access_key in aws_collect(client, 'iam.list_access_keys', params)
            ]
        }

    if method == 'get_login_profile':
        yield {
            'iam.get_login_profile': [
                updated(login_profile, account_info, {'user_name': params['UserName']})
                for login_profile in aws_collect(
                    client, 'iam.get_login_profile', params
                )
            ]
        }

    if method == 'list_mfa_devices':
        yield {
            'iam.list_mfa_devices': [
                updated(mfa_device, account_info, {'user_name': params['UserName']})
                for mfa_device in aws_collect(client, 'iam.list_mfa_devices', params)
            ]
        }

    if method == 'list_attached_user_policies':
        yield {
            'iam.list_attached_user_policies': [
                updated(user_policy, account_info, {'user_name': params['UserName']})
                for user_policy in aws_collect(
                    client, 'iam.list_attached_user_policies', params
                )
            ]
        }

    if method == 'list_user_policies':
        yield {
            'iam.list_user_policies': [
                updated(user_policy, account_info, {'user_name': params['UserName']})
                for user_policy in aws_collect(client, 'iam.list_user_policies', params)
            ]
        }

    if method == 'list_policies':
        policies = [
            updated(u, account_info) for u in aws_collect(client, 'iam.list_policies')
        ]
        yield {'iam.list_policies': policies}
        for policy_group in groups_of(1000, policies):
            add_task(
                {
                    'account_id': account_id,
                    'method': 'iam.get_policy_version',
                    'params': [
                        {
                            'PolicyArn': policy['arn'],
                            'VersionId': policy['default_version_id'],
                        }
                        for policy in policy_group
                    ],
                }
            )
            add_task(
                {
                    'account_id': account_id,
                    'method': 'iam.list_entities_for_policy',
                    'params': [{'PolicyArn': policy['arn']} for policy in policy_group],
                }
            )
    if method == 'get_policy_version':
        yield {
            'iam.get_policy_version': [
                updated(version, account_info, {'policy_arn': params['PolicyArn']})
                for version in aws_collect(client, 'iam.get_policy_version', params)
            ]
        }

    if method == 'list_entities_for_policy':
        yield {
            'iam.list_entities_for_policy': [
                updated(entity, account_info, {'policy_arn': params['PolicyArn']})
                for entity in aws_collect(
                    client, 'iam.list_entities_for_policy', params
                )
            ]
        }


def insert_list(name, values, table_name=None):
    name = name.replace('.', '_')
    table_name = table_name or f'data.aws_collect_{name}'
    log.info(f'inserting {len(values)} values into {table_name}')
    return db.insert(table_name, values)


def aws_collect_task(task, add_task=None):
    log.info(f'processing {task}')
    account_id = task['account_id']
    methods = task.get('methods') or [task['method']]
    params = task.get('params', {})
    if type(params) is dict:
        params = [params]

    for param in params:
        for method in methods:
            for lists in load_aws_iam(account_id, method, param, add_task):
                for name, values in lists.items():
                    response = insert_list(name, values)
                    log.info(f'finished {response}')


def ingest(table_name, options):
    global AUDIT_ASSUMER_ARN
    global MASTER_READER_ARN
    global AUDIT_READER_ROLE
    global READER_EID
    AUDIT_ASSUMER_ARN = options.get('audit_assumer_arn', '')
    MASTER_READER_ARN = options.get('master_reader_arn', '')
    AUDIT_READER_ROLE = options.get('audit_reader_role', '')
    READER_EID = options.get('reader_eid', '')

    org_client = sts_assume_role(
        src_role_arn=AUDIT_ASSUMER_ARN,
        dest_role_arn=MASTER_READER_ARN,
        dest_external_id=READER_EID,
    ).client('organizations')

    accounts = [a for a in aws_collect(org_client, 'organizations.list_accounts')]
    insert_list(
        'organizations.list_accounts', accounts, table_name=f'data.{table_name}'
    )
    if options.get('collect_apis') == 'all':
        qmap_mp(
            32,
            aws_collect_task,
            [
                {'method': method, 'account_id': a['id']}
                for a in accounts
                for method in [
                    'iam.get_account_summary',
                    'iam.get_account_password_policy',
                    'iam.list_users',
                    'iam.list_policies',
                    'iam.list_account_aliases',
                    's3.list_buckets',
                    'iam.generate_credential_report',
                    'iam.get_credential_report',
                    'iam.list_virtual_mfa_devices',
                    'ec2.describe_security_groups',
                    'cloudtrail.describe_trails',
                    'kms.list_keys',
                    'config.describe_configuration_recorders',
                    'ec2.describe_instances',
                ]
            ],
        )


def main(table_name, audit_assumer_ARN, master_reader_ARN, reader_eid, audit_reader_role):
    print(
        ingest(
            table_name,
            {
                'audit_assumer_ARN': audit_assumer_ARN,
                'master_reader_ARN': master_reader_ARN,
                'reader_eid': reader_eid,
                'audit_reader_role': audit_reader_role,
            },
        )
    )


if __name__ == '__main__':
    fire.Fire(main)
