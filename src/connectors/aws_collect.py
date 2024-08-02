"""AWS Inventory and Configuration
Load inventory from accounts in your Org via API using auditor Roles
"""

import asyncio
from botocore.exceptions import (
    # BotoCoreError,
    ClientError,
    DataNotFoundError,
)
from aiobotocore.config import AioConfig
from aiohttp.client_exceptions import ServerTimeoutError
from collections import defaultdict, namedtuple
import csv
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
import json
import fire
import io
import pytz
from typing import Tuple, AsyncGenerator, Dict, Any
from os import environ

from runners.helpers.dbconfig import DATA_SCHEMA, ROLE as SA_ROLE
from runners.utils import format_exception_only, format_exception

from connectors.utils import (
    aio_sts_assume_role,
    updated,
    yaml_dump,
    bytes_to_str,
    AioRateLimit,
)
from runners.helpers import db, log


AIO_CONFIG = AioConfig(
    read_timeout=600,
    connect_timeout=600,
)

AWS_ZONE = environ.get('SA_AWS_ZONE', 'aws')

AUDIT_ASSUMER_ARN = f'arn:{AWS_ZONE}:iam::111111111111:role/security-auditor'
AUDIT_READER_ROLE = 'audit-reader'
READER_EID = ''

_SESSION_CACHE: dict = {}

# see https://docs.aws.amazon.com/AWSEC2/latest/APIReference/throttling.html#throttling-limits
_REQUEST_PACE_PER_SECOND = 24  # depletes Throttling bucket of 100 at 4/s in 25s
_REQUEST_BATCH_SIZE = 100  # 100 in Throttling bucket + 500 replenished over 25s

# metadata API RPS limit https://github.com/aws/amazon-ecs-agent/blob/master/README.md#:~:text=ECS_TASK_METADATA_RPS_LIMIT
metadata_rate_limit = AioRateLimit(pace_per_second=40)

CONNECTION_OPTIONS = [
    {
        'type': 'str',
        'name': 'audit_assumer_arn',
        'title': "Audit Assumer ARN",
        'prompt': "The auditor role that assumes local roles in your accounts",
        'placeholder': f"arn:{AWS_ZONE}:iam::111111111111:role/security-auditor",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'org_account_ids',
        'title': "Master Account(s)",
        'prompt': "Comma-separated account id's to list-accounts in",
        'placeholder': "222222222222,333333333333",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'audit_reader_role',
        'title': "The name of the local auditor roles in your accounts",
        'prompt': "Role to be assumed for auditing the other accounts",
        'placeholder': "security-local-auditor",
        'required': True,
    },
    {
        'type': 'str',
        'name': 'reader_eid',
        'title': "Reader EID (optional)",
        'prompt': "External Id on the roles that need assuming",
        'secret': True,
    },
]

CollectTask = namedtuple('CollectTask', ['account_id', 'method', 'args'])
DBEntry = namedtuple('DBEntry', ['entity'])

ParsedCol = namedtuple('ParsedCol', ['type', 'colname', 'parsed_colname'])

PARSERS = {
    'csv': lambda v: [dict(x) for x in csv.DictReader(io.StringIO(v))],
    'json': json.loads,
}


LANDING_TABLE_COLUMNS = [
    ('recorded_at', 'TIMESTAMP_LTZ'),
    ('error', 'VARIANT'),
    ('id', 'STRING'),
    ('arn', 'STRING'),
    ('email', 'STRING'),
    ('name', 'STRING'),
    ('status', 'STRING'),
    ('joined_method', 'STRING'),
    ('joined_timestamp', 'TIMESTAMP_NTZ'),
]

SUPPLEMENTARY_TABLES = {
    # https://docs.aws.amazon.com/cli/latest/reference/iam/generate-credential-report.html#output
    'iam_generate_credential_report': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('state', 'STRING'),
        ('description', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-account-aliases.html#output
    'iam_list_account_aliases': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('account_alias', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/get-account-summary.html#output
    'iam_get_account_summary': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
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
    # https://docs.aws.amazon.com/cli/latest/reference/iam/get-account-password-policy.html#output
    'iam_get_account_password_policy': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('minimum_password_length', 'NUMBER'),
        ('require_symbols', 'BOOLEAN'),
        ('require_numbers', 'BOOLEAN'),
        ('require_uppercase_characters', 'BOOLEAN'),
        ('require_lowercase_characters', 'BOOLEAN'),
        ('allow_users_to_change_password', 'BOOLEAN'),
        ('expire_passwords', 'BOOLEAN'),
        ('max_password_age', 'NUMBER'),
        ('password_reuse_prevention', 'NUMBER'),
        ('hard_expiry', 'BOOLEAN'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-instances.html#output
    'ec2_describe_instances': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('error', 'VARIANT'),
        ('groups', 'VARIANT'),
        ('instances', 'VARIANT'),
        ('owner_id', 'STRING'),
        ('requester_id', 'STRING'),
        ('reservation_id', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-network-interfaces.html
    'ec2_describe_network_interfaces': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('error', 'VARIANT'),
        ('attachment', 'VARIANT'),
        ('association', 'VARIANT'),
        ('availability_zone', 'STRING'),
        ('description', 'STRING'),
        ('groups', 'VARIANT'),
        ('interface_type', 'STRING'),
        ('ipv6_addresses', 'VARIANT'),
        ('mac_address', 'STRING'),
        ('network_interface_id', 'STRING'),
        ('outpost_arn', 'STRING'),
        ('owner_id', 'STRING'),
        ('private_ip_address', 'STRING'),
        ('private_dns_name', 'STRING'),
        ('private_ip_addresses', 'VARIANT'),
        ('requester_id', 'STRING'),
        ('requester_managed', 'BOOLEAN'),
        ('source_dest_check', 'BOOLEAN'),
        ('status', 'STRING'),
        ('subnet_id', 'STRING'),
        ('tag_set', 'VARIANT'),
        ('vpc_id', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-nat-gateways.html
    'ec2_describe_nat_gateways': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('error', 'VARIANT'),
        ('nat_gateway_addresses', 'VARIANT'),
        ('vpc_id', 'STRING'),
        ('tags', 'VARIANT'),
        ('state', 'STRING'),
        ('nat_gateway_id', 'STRING'),
        ('subnet_id', 'STRING'),
        ('create_time', 'TIMESTAMP_LTZ'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-route-tables.html
    'ec2_describe_route_tables': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('error', 'VARIANT'),
        ('associations', 'VARIANT'),
        ('propagating_vgws', 'VARIANT'),
        ('route_table_id', 'STRING'),
        ('routes', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('vpc_id', 'STRING'),
        ('owner_id', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-security-groups.html#output
    'ec2_describe_security_groups': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('error', 'VARIANT'),
        ('description', 'STRING'),
        ('group_name', 'STRING'),
        ('ip_permissions', 'VARIANT'),
        ('owner_id', 'STRING'),
        ('group_id', 'STRING'),
        ('ip_permissions_egress', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('vpc_id', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/efs/describe-file-systems.html
    'efs_describe_file_systems': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('error', 'VARIANT'),
        ('owner_id', 'STRING'),
        ('creation_token', 'STRING'),
        ('file_system_id', 'STRING'),
        ('file_system_arn', 'STRING'),
        ('creation_time', 'TIMESTAMP_LTZ'),
        ('life_cycle_state', 'STRING'),
        ('name', 'STRING'),
        ('number_of_mount_targets', 'INTEGER'),
        ('size_in_bytes', 'VARIANT'),
        ('performance_mode', 'STRING'),
        ('encrypted', 'BOOLEAN'),
        ('kms_key_id', 'STRING'),
        ('throughput_mode', 'STRING'),
        ('provisioned_throughput_in_mibps', 'DOUBLE'),
        ('tags', 'VARIANT'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/efs/describe-mount-targets.html#examples
    'efs_describe_mount_targets': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('error', 'VARIANT'),
        ('owner_id', 'STRING'),
        ('mount_target_id', 'STRING'),
        ('file_system_id', 'STRING'),
        ('subnet_id', 'STRING'),
        ('life_cycle_state', 'STRING'),
        ('ip_address', 'STRING'),
        ('network_interface_id', 'STRING'),
        ('availability_zone_id', 'STRING'),
        ('availability_zone_name', 'STRING'),
        ('vpc_id', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/configservice/describe-configuration-recorders.html#output
    'config_describe_configuration_recorders': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('error', 'VARIANT'),
        ('name', 'STRING'),
        ('role_arn', 'STRING'),
        ('recording_group', 'VARIANT'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/get-credential-report.html#output
    'iam_get_credential_report': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('generated_time', 'TIMESTAMP_LTZ'),
        ('report_format', 'STRING'),
        ('content', 'STRING'),
        ('content_csv_parsed', 'VARIANT'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/kms/list-keys.html#output
    'kms_list_keys': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('error', 'VARIANT'),
        ('key_id', 'STRING'),
        ('key_arn', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/kms/get-key-rotation-status.html#output
    'kms_get_key_rotation_status': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('key_arn', 'STRING'),
        ('error', 'VARIANT'),
        ('key_rotation_enabled', 'BOOLEAN'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-users.html#output
    'iam_list_users': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('path', 'STRING'),
        ('user_name', 'STRING'),
        ('user_id', 'STRING'),
        ('arn', 'STRING'),
        ('create_date', 'TIMESTAMP_LTZ'),
        ('password_last_used', 'TIMESTAMP_LTZ'),
        ('permissions_boundary', 'VARIANT'),
        ('tags', 'VARIANT'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/get-login-profile.html#output
    'iam_get_login_profile': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('user_name', 'STRING'),
        ('error', 'VARIANT'),
        ('create_date', 'TIMESTAMP_LTZ'),
        ('password_reset_required', 'BOOLEAN'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-mfa-devices.html#output
    'iam_list_mfa_devices': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('user_name', 'STRING'),
        ('error', 'VARIANT'),
        ('serial_number', 'STRING'),
        ('enable_date', 'TIMESTAMP_LTZ'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-access-keys.html#output
    'iam_list_access_keys': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('user_name', 'STRING'),
        ('error', 'VARIANT'),
        ('access_key_id', 'STRING'),
        ('status', 'STRING'),
        ('create_date', 'TIMESTAMP_LTZ'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-groups-for-user.html#output
    'iam_list_groups_for_user': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('user_name', 'STRING'),
        ('error', 'VARIANT'),
        ('path', 'STRING'),
        ('group_name', 'STRING'),
        ('group_id', 'STRING'),
        ('arn', 'STRING'),
        ('create_date', 'TIMESTAMP_LTZ'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-user-policies.html#output
    'iam_list_user_policies': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('user_name', 'STRING'),
        ('error', 'VARIANT'),
        ('policy_name', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/get-user-policy.html#output
    'iam_get_user_policy': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('user_name', 'STRING'),
        ('policy_name', 'STRING'),
        ('error', 'VARIANT'),
        ('policy_document', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-attached-user-policies.html#output
    'iam_list_attached_user_policies': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('user_name', 'STRING'),
        ('error', 'VARIANT'),
        ('policy_name', 'STRING'),
        ('policy_arn', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-groups.html#output
    'iam_list_groups': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('path', 'STRING'),
        ('group_id', 'STRING'),
        ('group_name', 'STRING'),
        ('arn', 'STRING'),
        ('create_date', 'TIMESTAMP_LTZ'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-group-policies.html#output
    'iam_list_group_policies': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('group_name', 'STRING'),
        ('error', 'VARIANT'),
        ('policy_name', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/get-group-policy.html#output
    'iam_get_group_policy': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('group_name', 'STRING'),
        ('policy_name', 'STRING'),
        ('error', 'VARIANT'),
        ('policy_document', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-attached-group-policies.html#output
    'iam_list_attached_group_policies': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('group_name', 'STRING'),
        ('error', 'VARIANT'),
        ('policy_name', 'STRING'),
        ('policy_arn', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-roles.html#output
    'iam_list_roles': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('path', 'STRING'),
        ('role_name', 'STRING'),
        ('role_id', 'STRING'),
        ('arn', 'STRING'),
        ('create_date', 'TIMESTAMP_LTZ'),
        ('assume_role_policy_document', 'STRING'),
        ('description', 'STRING'),
        ('max_session_duration', 'NUMBER'),
        ('permissions_boundary_type', 'VARIANT'),
        ('permissions_boundary_arn', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('role_last_used', 'VARIANT'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-role-policies.html#output
    'iam_list_role_policies': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('role_name', 'STRING'),
        ('error', 'VARIANT'),
        ('policy_name', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/get-role-policy.html#output
    'iam_get_role_policy': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('role_name', 'STRING'),
        ('policy_name', 'STRING'),
        ('error', 'VARIANT'),
        ('policy_document', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-attached-role-policies.html#output
    'iam_list_attached_role_policies': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('role_name', 'STRING'),
        ('error', 'VARIANT'),
        ('policy_name', 'STRING'),
        ('policy_arn', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-policies.html#output
    'iam_list_policies': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('policy_name', 'STRING'),
        ('policy_id', 'STRING'),
        ('arn', 'STRING'),
        ('path', 'STRING'),
        ('default_version_id', 'STRING'),
        ('attachment_count', 'NUMBER'),
        ('permissions_boundary_usage_count', 'NUMBER'),
        ('is_attachable', 'BOOLEAN'),
        ('description', 'STRING'),
        ('create_date', 'TIMESTAMP_LTZ'),
        ('update_date', 'TIMESTAMP_LTZ'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/get-policy-version.html#output
    'iam_get_policy_version': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('policy_arn', 'STRING'),
        ('version_id', 'STRING'),
        ('error', 'VARIANT'),
        ('document', 'STRING'),
        ('is_default_version', 'BOOLEAN'),
        ('create_date', 'TIMESTAMP_LTZ'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-entities-for-policy.html#output
    'iam_list_entities_for_policy': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('policy_arn', 'STRING'),
        ('error', 'VARIANT'),
        ('group_id', 'STRING'),
        ('group_name', 'STRING'),
        ('user_id', 'STRING'),
        ('user_name', 'STRING'),
        ('role_id', 'STRING'),
        ('role_name', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/list-virtual-mfa-devices.html#output
    'iam_list_virtual_mfa_devices': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('serial_number', 'STRING'),
        ('base32_string_seed', 'STRING'),
        ('qr_code_png', 'STRING'),
        ('user', 'VARIANT'),
        ('enable_date', 'TIMESTAMP_LTZ'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/s3api/list-buckets.html#output
    's3_list_buckets': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('bucket_name', 'STRING'),
        ('bucket_creation_date', 'TIMESTAMP_LTZ'),
        ('owner_display_name', 'STRING'),
        ('owner_id', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/s3api/get-bucket-acl.html#output
    's3_get_bucket_acl': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('bucket', 'STRING'),
        ('grants_grantee', 'STRING'),
        ('grants_permission', 'STRING'),
        ('owner_display_name', 'STRING'),
        ('owner_id', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/s3api/get-bucket-policy.html#output
    's3_get_bucket_policy': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('bucket', 'STRING'),
        ('error', 'VARIANT'),
        ('policy', 'STRING'),
        ('policy_json_parsed', 'VARIANT'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/s3api/get-bucket-logging.html#output
    's3_get_bucket_logging': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('bucket', 'STRING'),
        ('error', 'VARIANT'),
        ('target_bucket', 'STRING'),
        ('target_grants', 'VARIANT'),
        ('target_prefix', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/s3api/get-bucket-tagging.html#output
    's3_get_bucket_tagging': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('bucket', 'STRING'),
        ('error', 'VARIANT'),
        ('tag_set', 'VARIANT'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/s3api/get-public-access-block.html
    's3_get_public_access_block': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('bucket', 'STRING'),
        ('error', 'VARIANT'),
        ('block_public_acls', 'BOOLEAN'),
        ('ignore_public_acls', 'BOOLEAN'),
        ('block_public_policy', 'BOOLEAN'),
        ('restrict_public_buckets', 'BOOLEAN'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/s3control/get-public-access-block.html
    's3control_get_public_access_block': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('block_public_acls', 'BOOLEAN'),
        ('ignore_public_acls', 'BOOLEAN'),
        ('block_public_policy', 'BOOLEAN'),
        ('restrict_public_buckets', 'BOOLEAN'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/cloudtrail/describe-trails.html#output
    'cloudtrail_describe_trails': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
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
        ('has_insight_selectors', 'BOOLEAN'),
        ('is_organization_trail', 'BOOLEAN'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/cloudtrail/get-trail-status.html#output
    'cloudtrail_get_trail_status': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('trail_arn', 'STRING'),
        ('error', 'VARIANT'),
        ('is_logging', 'BOOLEAN'),
        ('latest_delivery_error', 'STRING'),
        ('latest_notification_error', 'STRING'),
        ('latest_delivery_time', 'TIMESTAMP_NTZ'),
        ('latest_notification_time', 'TIMESTAMP_NTZ'),
        ('start_logging_time', 'TIMESTAMP_NTZ'),
        ('stop_logging_time', 'TIMESTAMP_NTZ'),
        ('latest_cloud_watch_logs_delivery_error', 'STRING'),
        ('latest_cloud_watch_logs_delivery_time', 'TIMESTAMP_NTZ'),
        ('latest_digest_delivery_time', 'TIMESTAMP_NTZ'),
        ('latest_digest_delivery_error', 'STRING'),
        ('latest_delivery_attempt_time', 'STRING'),
        ('latest_notification_attempt_time', 'STRING'),
        ('latest_notification_attempt_succeeded', 'STRING'),
        ('latest_delivery_attempt_succeeded', 'STRING'),
        ('time_logging_started', 'STRING'),
        ('time_logging_stopped', 'STRING'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/cloudtrail/get-event-selectors.html#output
    'cloudtrail_get_event_selectors': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('trail_arn', 'STRING'),
        ('read_write_type', 'STRING'),
        ('include_management_events', 'BOOLEAN'),
        ('data_resources', 'VARIANT'),
        ('exclude_management_event_sources', 'VARIANT'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/inspector/list-findings.html
    'inspector_list_findings': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('error', 'VARIANT'),
        ('finding_arns', 'VARIANT'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/inspector/describe-findings.html
    'inspector_describe_findings': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('finding_arns', 'STRING'),
        ('failed_items', 'VARIANT'),
        ('arn', 'STRING'),
        ('schema_version', 'INTEGER'),
        ('service', 'STRING'),
        ('service_attributes', 'VARIANT'),
        ('asset_type', 'STRING'),
        ('asset_attributes', 'VARIANT'),
        ('id', 'STRING'),
        ('title', 'STRING'),
        ('description', 'STRING'),
        ('recommendation', 'STRING'),
        ('severity', 'STRING'),
        ('numeric_severity', 'DOUBLE'),
        ('confidence', 'INTEGER'),
        ('indicator_of_compromise', 'BOOLEAN'),
        ('attributes', 'VARIANT'),
        ('user_attributes', 'VARIANT'),
        ('created_at', 'TIMESTAMP_NTZ'),
        ('updated_at', 'TIMESTAMP_NTZ'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-snapshots.html#output
    'ec2_describe_snapshots': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('region', 'STRING'),
        ('error', 'VARIANT'),
        ('data_encryption_key_id', 'STRING'),
        ('description', 'STRING'),
        ('encrypted', 'BOOLEAN'),
        ('kms_key_id', 'STRING'),
        ('owner_id', 'STRING'),
        ('progress', 'STRING'),
        ('snapshot_id', 'STRING'),
        ('start_time', 'TIMESTAMP_LTZ'),
        ('state', 'STRING'),
        ('state_message', 'STRING'),
        ('volume_id', 'STRING'),
        ('volume_size', 'NUMBER'),
        ('owner_alias', 'STRING'),
        ('outpost_arn', 'STRING'),
        ('tags', 'VARIANT'),
    ],
    # https://docs.aws.amazon.com/cli/latest/reference/iam/get-account-authorization-details.html
    'iam_get_account_authorization_details': [
        ('recorded_at', 'TIMESTAMP_LTZ'),
        ('account_id', 'STRING'),
        ('error', 'VARIANT'),
        ('arn', 'STRING'),
        ('assume_role_policy_document', 'VARIANT'),
        ('attached_managed_policies', 'VARIANT'),
        ('attachment_count', 'NUMBER'),
        ('create_date', 'TIMESTAMP'),
        ('default_version_id', 'STRING'),
        ('description', 'STRING'),
        ('group_id', 'STRING'),
        ('group_list', 'VARIANT'),
        ('group_name', 'STRING'),
        ('group_policy_list', 'VARIANT'),
        ('instance_profile_list', 'VARIANT'),
        ('is_attachable', 'BOOLEAN'),
        ('path', 'STRING'),
        ('permissions_boundary', 'VARIANT'),
        ('permissions_boundary_usage_count', 'NUMBER'),
        ('policy_id', 'STRING'),
        ('policy_name', 'STRING'),
        ('policy_version_list', 'VARIANT'),
        ('role_id', 'STRING'),
        ('role_last_used', 'VARIANT'),
        ('role_name', 'STRING'),
        ('role_policy_list', 'VARIANT'),
        ('tags', 'VARIANT'),
        ('update_date', 'TIMESTAMP'),
        ('user_id', 'STRING'),
        ('user_name', 'STRING'),
        ('user_policy_list', 'VARIANT'),
    ],
}

API_METHOD_SPECS: Dict[str, dict] = {
    'organizations.list_accounts': {
        'response': {
            'Accounts': [
                {
                    'Id': 'id',
                    'Arn': 'arn',
                    'Email': 'email',
                    'Name': 'name',
                    'Status': 'status',
                    'JoinedMethod': 'joined_method',
                    'JoinedTimestamp': 'joined_timestamp',
                }
            ]
        }
    },
    'iam.list_account_aliases': {'response': {'AccountAliases': ['account_alias']}},
    'iam.get_account_summary': {
        'response': {
            'SummaryMap': {
                'GroupPolicySizeQuota': 'group_policy_size_quota',
                'InstanceProfilesQuota': 'instance_profiles_quota',
                'Policies': 'policies',
                'GroupsPerUserQuota': 'groups_per_user_quota',
                'InstanceProfiles': 'instance_profiles',
                'AttachedPoliciesPerUserQuota': 'attached_policies_per_user_quota',
                'Users': 'users',
                'PoliciesQuota': 'policies_quota',
                'Providers': 'providers',
                'AccountMFAEnabled': 'account_mfa_enabled',
                'AccessKeysPerUserQuota': 'access_keys_per_user_quota',
                'AssumeRolePolicySizeQuota': 'assume_role_policy_size_quota',
                'PolicyVersionsInUseQuota': 'policy_versions_in_use_quota',
                'GlobalEndpointTokenVersion': 'global_endpoint_token_version',
                'VersionsPerPolicyQuota': 'versions_per_policy_quota',
                'AttachedPoliciesPerGroupQuota': 'attached_policies_per_group_quota',
                'PolicySizeQuota': 'policy_size_quota',
                'Groups': 'groups',
                'AccountSigningCertificatesPresent': 'account_signing_certificates_present',
                'UsersQuota': 'users_quota',
                'ServerCertificatesQuota': 'server_certificates_quota',
                'MFADevices': 'mfa_devices',
                'UserPolicySizeQuota': 'user_policy_size_quota',
                'PolicyVersionsInUse': 'policy_versions_in_use',
                'ServerCertificates': 'server_certificates',
                'Roles': 'roles',
                'RolesQuota': 'roles_quota',
                'SigningCertificatesPerUserQuota': 'signing_certificates_per_user_quota',
                'MFADevicesInUse': 'mfa_devices_in_use',
                'RolePolicySizeQuota': 'role_policy_size_quota',
                'AttachedPoliciesPerRoleQuota': 'attached_policies_per_role_quota',
                'AccountAccessKeysPresent': 'account_access_keys_present',
                'GroupsQuota': 'groups_quota',
            }
        }
    },
    'iam.get_account_password_policy': {
        'response': {
            'PasswordPolicy': {
                'MinimumPasswordLength': 'minimum_password_length',
                'RequireSymbols': 'require_symbols',
                'RequireNumbers': 'require_numbers',
                'RequireUppercaseCharacters': 'require_uppercase_characters',
                'RequireLowercaseCharacters': 'require_lowercase_characters',
                'AllowUsersToChangePassword': 'allow_users_to_change_password',
                'ExpirePasswords': 'expire_passwords',
                'MaxPasswordAge': 'max_password_age',
                'PasswordReusePrevention': 'password_reuse_prevention',
                'HardExpiry': 'hard_expiry',
            }
        }
    },
    'ec2.describe_instances': {
        'response': {
            'Reservations': [
                {
                    'Groups': 'groups',
                    'Instances': 'instances',
                    'OwnerId': 'owner_id',
                    'RequesterId': 'requester_id',
                    'ReservationId': 'reservation_id',
                }
            ]
        }
    },
    'ec2.describe_network_interfaces': {
        'response': {
            'NetworkInterfaces': [
                {
                    'Groups': 'groups',
                    'InterfaceType': 'interface_type',
                    'Ipv6Addresses': 'ipv6_addresses',
                    'RequesterId': 'requester_id',
                    'Attachment': 'attachment',
                    'Association': 'attachment',
                    'AvailabilityZone': 'availability_zone',
                    'Description': 'description',
                    'MacAddress': 'mac_address',
                    'NetworkInterfaceId': 'network_interface_id',
                    'OutpostArn': 'outpost_arn',
                    'OwnerId': 'owner_id',
                    'PrivateIpAddress': 'private_ip_address',
                    'PrivateDnsName': 'private_dns_name',
                    'PrivateIpAddresses': 'private_ip_addresses',
                    'RequesterManaged': 'requester_managed',
                    'SourceDestCheck': 'source_dest_check',
                    'Status': 'status',
                    'SubnetId': 'subnet_id',
                    'TagSet': 'tag_set',
                    'VpcId': 'vpc_id',
                }
            ]
        }
    },
    'ec2.describe_nat_gateways': {
        'response': {
            'NatGateways': [
                {
                    'NatGatewayAddresses': 'nat_gateway_addresses',
                    'VpcId': 'vpc_id',
                    'Tags': 'tags',
                    'State': 'state',
                    'NatGatewayId': 'nat_gateway_id',
                    'SubnetId': 'subnet_id',
                    'CreateTime': 'create_time',
                }
            ]
        }
    },
    'ec2.describe_route_tables': {
        'response': {
            'RouteTables': [
                {
                    'Associations': 'associations',
                    'PropagatingVgws': 'propagating_vgws',
                    'RouteTableId': 'route_table_id',
                    'Routes': 'routes',
                    'Tags': 'tags',
                    'VpcId': 'vpc_id',
                    'OwnerId': 'owner_id',
                }
            ]
        }
    },
    'ec2.describe_security_groups': {
        'response': {
            'SecurityGroups': [
                {
                    'Description': 'description',
                    'GroupName': 'group_name',
                    'IpPermissions': 'ip_permissions',
                    'OwnerId': 'owner_id',
                    'GroupId': 'group_id',
                    'IpPermissionsEgress': 'ip_permissions_egress',
                    'Tags': 'tags',
                    'VpcId': 'vpc_id',
                }
            ]
        }
    },
    'efs.describe_file_systems': {
        'response': {
            'FileSystems': [
                {
                    'OwnerId': 'owner_id',
                    'CreationToken': 'creation_token',
                    'FileSystemId': 'file_system_id',
                    'FileSystemArn': 'file_system_arn',
                    'CreationTime': 'creation_time',
                    'LifeCycleState': 'life_cycle_state',
                    'Name': 'name',
                    'NumberOfMountTargets': 'number_of_mount_targets',
                    'SizeInBytes': 'size_in_bytes',
                    'PerformanceMode': 'performance_mode',
                    'Encrypted': 'encrypted',
                    'KmsKeyId': 'kms_key_id',
                    'ThroughputMode': 'throughput_mode',
                    'ProvisionedThroughputInMibps': 'provisioned_throughput_in_mibps',
                    'Tags': 'tags',
                }
            ]
        },
        'children': [
            {
                'method': 'efs.describe_mount_targets',
                'args': {'FileSystemId': 'file_system_id'},
            }
        ],
    },
    'efs.describe_mount_targets': {
        'params': {'FileSystemId': 'file_system_id'},
        'response': {
            'MountTargets': [
                {
                    'OwnerId': 'owner_id',
                    'MountTargetId': 'mount_target_id',
                    'FileSystemId': 'file_system_id',
                    'SubnetId': 'subnet_id',
                    'LifeCycleState': 'life_cycle_state',
                    'IpAddress': 'ip_address',
                    'NetworkInterfaceId': 'network_interface_id',
                    'AvailabilityZoneId': 'availability_zone_id',
                    'AvailabilityZoneName': 'availability_zone_name',
                    'VpcId': 'vpc_id',
                }
            ]
        },
    },
    'config.describe_configuration_recorders': {
        # for unknown reasons, client.describe_regions does not seem to work w/
        # Config client. seems like a boto3 bug. the below is a work-around.
        'regions': [
            'us-east-1',
            'us-east-2',
            'us-west-1',
            'us-west-2',
            'ap-south-1',
            'ap-northeast-2',
            'ap-southeast-2',
            'ap-northeast-1',
            'eu-central-1',
            'eu-west-1',
            'eu-west-2',
            'eu-north-1',
        ],
        'response': {
            'ConfigurationRecorders': [
                {
                    'name': 'name',
                    'roleARN': 'role_arn',
                    'recordingGroup': 'recording_group',
                }
            ]
        },
    },
    'kms.list_keys': {
        'response': {'Keys': [{'KeyId': 'key_id', 'KeyArn': 'key_arn'}]},
        'children': [
            {'method': 'kms.get_key_rotation_status', 'args': {'KeyId': 'key_arn'}}
        ],
    },
    'kms.get_key_rotation_status': {
        'params': {'KeyId': 'key_arn'},
        'response': {'KeyRotationEnabled': 'key_rotation_enabled'},
    },
    'iam.generate_credential_report': {
        'response': {'State': 'state', 'Description': 'description'}
    },
    'iam.get_credential_report': {
        'response': {
            'Content': ParsedCol('csv', 'content', 'content_csv_parsed'),
            'ReportFormat': 'report_format',
            'GeneratedTime': 'generated_time',
        }
    },
    'iam.list_groups': {
        'response': {
            'Groups': [
                {
                    'Arn': 'arn',
                    'Path': 'path',
                    'CreateDate': 'create_date',
                    'GroupId': 'group_id',
                    'GroupName': 'group_name',
                }
            ]
        },
        'children': [
            {
                'methods': [
                    'iam.list_attached_group_policies',
                    'iam.list_group_policies',
                ],
                'args': {'GroupName': 'group_name'},
            }
        ],
    },
    'iam.list_group_policies': {
        'params': {'GroupName': 'group_name'},
        'response': {'PolicyNames': ['policy_name']},
        'children': [
            {
                'method': 'iam.get_group_policy',
                'args': {'GroupName': 'group_name', 'PolicyName': 'policy_name'},
            }
        ],
    },
    'iam.get_group_policy': {
        'params': {'GroupName': 'group_name', 'PolicyName': 'policy_name'},
        'response': {'PolicyDocument': 'policy_document'},
    },
    'iam.list_users': {
        'response': {
            'Users': [
                {
                    'Arn': 'arn',
                    'Path': 'path',
                    'CreateDate': 'create_date',
                    'UserId': 'user_id',
                    'UserName': 'user_name',
                    'PasswordLastUsed': 'password_last_used',
                    'PermissionsBoundary': 'permissions_boundary',
                    'Tags': 'tags',
                }
            ]
        },
        'children': [
            {
                'methods': [
                    'iam.get_login_profile',
                    'iam.list_mfa_devices',
                    'iam.list_access_keys',
                    'iam.list_groups_for_user',
                    'iam.list_user_policies',
                    'iam.list_attached_user_policies',
                ],
                'args': {'UserName': 'user_name'},
            }
        ],
    },
    'iam.list_groups_for_user': {
        'params': {'UserName': 'user_name'},
        'response': {
            'Groups': [
                {
                    'Path': 'path',
                    'GroupName': 'group_name',
                    'GroupId': 'group_id',
                    'Arn': 'arn',
                    'CreateDate': 'create_date',
                }
            ]
        },
    },
    'iam.list_access_keys': {
        'params': {'UserName': 'user_name'},
        'response': {
            'AccessKeyMetadata': [
                {
                    'UserName': 'user_name',
                    'AccessKeyId': 'access_key_id',
                    'Status': 'status',
                    'CreateDate': 'create_date',
                }
            ]
        },
    },
    'iam.get_login_profile': {
        'params': {'UserName': 'user_name'},
        'response': {
            'LoginProfile': {
                'UserName': 'user_name',
                'CreateDate': 'create_date',
                'PasswordResetRequired': 'password_reset_required',
            }
        },
    },
    'iam.list_mfa_devices': {
        'params': {'UserName': 'user_name'},
        'response': {
            'MFADevices': [
                {
                    'UserName': 'user_name',
                    'SerialNumber': 'serial_number',
                    'EnableDate': 'enable_date',
                }
            ]
        },
    },
    'iam.list_user_policies': {
        'params': {'UserName': 'user_name'},
        'response': {'PolicyNames': ['policy_name']},
        'children': [
            {
                'method': 'iam.get_user_policy',
                'args': {'UserName': 'user_name', 'PolicyName': 'policy_name'},
            }
        ],
    },
    'iam.get_user_policy': {
        'params': {'UserName': 'user_name', 'PolicyName': 'policy_name'},
        'response': {'PolicyDocument': 'policy_document'},
    },
    'iam.list_attached_user_policies': {
        'params': {'UserName': 'user_name'},
        'response': {
            'AttachedPolicies': [
                {'PolicyName': 'policy_name', 'PolicyArn': 'policy_arn'}
            ]
        },
    },
    'iam.list_attached_group_policies': {
        'params': {'GroupName': 'group_name'},
        'response': {
            'AttachedPolicies': [
                {'PolicyName': 'policy_name', 'PolicyArn': 'policy_arn'}
            ]
        },
    },
    'iam.list_attached_role_policies': {
        'params': {'RoleName': 'role_name'},
        'response': {
            'AttachedPolicies': [
                {'PolicyName': 'policy_name', 'PolicyArn': 'policy_arn'}
            ]
        },
    },
    'iam.list_roles': {
        'response': {
            'Roles': [
                {
                    'Path': 'path',
                    'RoleName': 'role_name',
                    'RoleId': 'role_id',
                    'Arn': 'arn',
                    'CreateDate': 'create_date',
                    'AssumeRolePolicyDocument': 'assume_role_policy_document',
                    'Description': 'description',
                    'MaxSessionDuration': 'max_session_duration',
                    'PermissionsBoundary': {
                        'PermissionsBoundaryType': 'permissions_boundary_type',
                        'PermissionsBoundaryArn': 'permissions_boundary_arn',
                    },
                    'Tags': 'tags',
                    'RoleLastUsed': 'role_last_used',
                }
            ]
        },
        'children': [
            {
                'methods': [
                    'iam.list_role_policies',
                    'iam.list_attached_role_policies',
                ],
                'args': {'RoleName': 'role_name'},
            }
        ],
    },
    'iam.list_role_policies': {
        'params': {'RoleName': 'role_name'},
        'response': {'PolicyNames': ['policy_name']},
        'children': [
            {
                'method': 'iam.get_role_policy',
                'args': {'RoleName': 'role_name', 'PolicyName': 'policy_name'},
            }
        ],
    },
    'iam.get_role_policy': {
        'params': {'RoleName': 'role_name', 'PolicyName': 'policy_name'},
        'response': {'PolicyDocument': 'policy_document'},
    },
    'iam.list_policies': {
        'response': {
            'Policies': [
                {
                    'PolicyName': 'policy_name',
                    'PolicyId': 'policy_id',
                    'Arn': 'arn',
                    'Path': 'path',
                    'DefaultVersionId': 'default_version_id',
                    'AttachmentCount': 'attachment_count',
                    'PermissionsBoundaryUsageCount': 'permissions_boundary_usage_count',
                    'IsAttachable': 'is_attachable',
                    'Description': 'description',
                    'CreateDate': 'create_date',
                    'UpdateDate': 'update_date',
                }
            ]
        },
        'children': [
            {
                'method': 'iam.get_policy_version',
                'args': {'PolicyArn': 'arn', 'VersionId': 'default_version_id'},
            },
            {'method': 'iam.list_entities_for_policy', 'args': {'PolicyArn': 'arn'}},
        ],
    },
    'iam.get_policy_version': {
        'params': {'PolicyArn': 'policy_arn'},
        'response': {
            'PolicyVersion': {
                'Document': 'document',
                'VersionId': 'version_id',
                'CreateDate': 'create_date',
                'IsDefaultVersion': 'is_default_version',
            }
        },
    },
    'iam.list_entities_for_policy': {
        'params': {'PolicyArn': 'policy_arn'},
        'response': {
            'PolicyGroups': [{'GroupName': 'group_name', 'GroupId': 'group_id'}],
            'PolicyUsers': [{'UserName': 'user_name', 'UserId': 'user_id'}],
            'PolicyRoles': [{'RoleName': 'role_name', 'RoleId': 'role_id'}],
        },
    },
    'iam.list_virtual_mfa_devices': {
        'response': {
            'VirtualMFADevices': [
                {
                    'SerialNumber': 'serial_number',
                    'Base32StringSeed': 'base32_string_seed',
                    'QRCodePNG': 'qr_code_png',
                    'User': 'user',
                    'EnableDate': 'enable_date',
                }
            ]
        }
    },
    's3.list_buckets': {
        'response': {
            'Buckets': [
                {'Name': 'bucket_name', 'CreationDate': 'bucket_creation_date'}
            ],
            'Owner': {'DisplayName': 'owner_display_name', 'ID': 'owner_id'},
        },
        'children': [
            {
                'methods': [
                    's3.get_bucket_acl',
                    's3.get_bucket_policy',
                    's3.get_bucket_logging',
                    's3.get_bucket_tagging',
                    's3.get_public_access_block',
                ],
                'args': {'Bucket': 'bucket_name'},
            }
        ],
    },
    's3.get_bucket_acl': {
        'params': {'Bucket': 'bucket'},
        'response': {
            'Owner': {'DisplayName': 'owner_display_name', 'ID': 'owner_id'},
            'Grants': [
                {'Grantee': 'grants_grantee', 'Permission': 'grants_permission'}
            ],
        },
    },
    's3.get_bucket_policy': {
        'params': {'Bucket': 'bucket'},
        'response': {'Policy': ParsedCol('json', 'policy', 'policy_json_parsed')},
    },
    's3.get_bucket_logging': {
        'params': {'Bucket': 'bucket'},
        'response': {
            'LoggingEnabled': {
                'TargetBucket': 'target_bucket',
                'TargetGrants': 'target_grants',
                'TargetPrefix': 'target_prefix',
            }
        },
    },
    's3.get_bucket_tagging': {
        'params': {'Bucket': 'bucket'},
        'response': {'TagSet': 'tag_set'},
    },
    's3.get_public_access_block': {
        'params': {'Bucket': 'bucket'},
        'response': {
            'PublicAccessBlockConfiguration': {
                'BlockPublicAcls': 'block_public_acls',
                'IgnorePublicAcls': 'ignore_public_acls',
                'BlockPublicPolicy': 'block_public_policy',
                'RestrictPublicBuckets': 'restrict_public_buckets',
            }
        },
    },
    's3control.get_public_access_block': {
        'args': {'AccountId': 'account_id'},
        'response': {
            'PublicAccessBlockConfiguration': {
                'BlockPublicAcls': 'block_public_acls',
                'IgnorePublicAcls': 'ignore_public_acls',
                'BlockPublicPolicy': 'block_public_policy',
                'RestrictPublicBuckets': 'restrict_public_buckets',
            }
        },
    },
    'cloudtrail.describe_trails': {
        'response': {
            'trailList': [
                {
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
                    'HasInsightSelectors': 'has_insight_selectors',
                    'IsOrganizationTrail': 'is_organization_trail',
                }
            ]
        },
        'children': [
            {'method': 'cloudtrail.get_trail_status', 'args': {'Name': 'trail_arn'}},
            {
                'method': 'cloudtrail.get_event_selectors',
                'args': {'TrailName': 'trail_arn'},
            },
        ],
    },
    'cloudtrail.get_trail_status': {
        'params': {'Name': 'trail_arn'},
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
        },
    },
    'cloudtrail.get_event_selectors': {
        'response': {
            'TrailARN': 'trail_arn',
            'EventSelectors': [
                {
                    'ReadWriteType': 'read_write_type',
                    'IncludeManagementEvents': 'include_management_events',
                    'DataResources': 'data_resources',
                    'ExcludeManagementEventSources': 'exclude_management_event_sources',
                }
            ],
        }
    },
    'inspector.list_findings': {
        # for unknown reasons, client.describe_regions does not seem to work w/
        # Inspector client. seems like a boto3 bug. the below is a work-around.
        'regions': [
            'us-east-1',
            'us-east-2',
            'us-west-1',
            'us-west-2',
            'ap-south-1',
            'ap-northeast-2',
            'ap-southeast-2',
            'ap-northeast-1',
            'eu-central-1',
            'eu-west-1',
            'eu-west-2',
            'eu-north-1',
        ],
        'response': {'findingArns': 'finding_arns'},
        'children': [
            {
                'method': 'inspector.describe_findings',
                'args': {'findingArns': 'finding_arns'},
                'required_args': ['finding_arns'],
            }
        ],
    },
    'inspector.describe_findings': {
        'params': {'findingArns': 'finding_arns'},
        'response': {
            'failedItems': 'failed_items',
            'findings': [
                {
                    'arn': 'arn',
                    'schemaVersion': 'schema_version',
                    'service': 'service',
                    'serviceAttributes': 'service_attributes',
                    'assetType': 'asset_type',
                    'assetAttributes': 'asset_attributes',
                    'id': 'id',
                    'title': 'title',
                    'description': 'description',
                    'recommendation': 'recommendation',
                    'severity': 'severity',
                    'numericSeverity': 'numeric_severity',
                    'confidence': 'confidence',
                    'indicatorOfCompromise': 'indicator_of_compromise',
                    'attributes': 'attributes',
                    'userAttributes': 'user_attributes',
                    'createdAt': 'created_at',
                    'updatedAt': 'updated_at',
                    'arn': 'arn',
                    'assetAttributes': 'asset_attributes',
                    'assetType': 'asset_type',
                    'attributes': 'attributes',
                    'confidence': 'confidence',
                    'createdAt': 'created_at',
                    'description': 'description',
                    'indicatorOfCompromise': 'indicator_of_compromise',
                    'numericSeverity': 'numeric_severity',
                    'recommendation': 'recommendation',
                    'schemaVersion': 'schema_version',
                    'service': 'service',
                    'serviceAttributes': 'service_attributes',
                    'severity': 'severity',
                    'title': 'title',
                    'userAttributes': 'user_attributes',
                }
            ],
        },
    },
    'ec2.describe_snapshots': {
        'response': {
            'Snapshot': [
                {
                    'DataEncryptionKeyId': 'data_encryption_key_id',
                    'Description': 'description',
                    'Encrypted': 'encrypted',
                    'KmsKeyID': 'kms_key_id',
                    'OwnerID': 'owner_id',
                    'Progress': 'progress',
                    'SnapshotID': 'snapshot_id',
                    'StartTime': 'start_time',
                    'State': 'state',
                    'StateMessage': 'state_message',
                    'VolumeID': 'volume_id',
                    'VolumeSize': 'volume_size',
                    'OwnerAlias': 'owner_alias',
                    'OutpostArn': 'outpost_arn',
                    'Tags': 'tags',
                }
            ]
        },
    },
    'iam.get_account_authorization_details': {
        'response': {
            'UserDetailList': [
                {
                    'Path': 'path',
                    'UserName': 'user_name',
                    'UserId': 'user_id',
                    'Arn': 'arn',
                    'CreateDate': 'create_date',
                    'UserPolicyList': 'user_policy_list',
                    'GroupList': 'group_list',
                    'AttachedManagedPolicies': 'attached_managed_policies',
                    'PermissionsBoundary': 'permissions_boundary',
                    'Tags': 'tags',
                }
            ],
            'GroupDetailList': [
                {
                    'Path': 'path',
                    'GroupName': 'group_name',
                    'GroupId': 'group_id',
                    'Arn': 'arn',
                    'CreateDate': 'create_date',
                    'GroupPolicyList': 'group_policy_list',
                    'AttachedManagedPolicies': 'attached_managed_policies',
                }
            ],
            'RoleDetailList': [
                {
                    'RoleName': 'role_name',
                    'RoleId': 'role_id',
                    'Arn': 'arn',
                    'CreateDate': 'create_date',
                    'AssumeRolePolicyDocument': 'assume_role_policy_document',
                    'InstanceProfileList': 'instance_profile_list',
                    'RolePolicyList': 'role_policy_list',
                    'AttachedManagedPolicies': 'attached_managed_policies',
                    'PermissionsBoundary': 'permissions_boundary',
                    'Tags': 'tags',
                    'RoleLastUsed': 'role_last_used',
                }
            ],
            'Policies': [
                {
                    'PolicyName': 'policy_name',
                    'PolicyId': 'policy_id',
                    'Arn': 'arn',
                    'Path': 'path',
                    'DefaultVersionId': 'default_version_id',
                    'AttachmentCount': 'attachment_count',
                    'PermissionsBoundaryUsageCount': 'permissions_boundary_usage_count',
                    'IsAttachable': 'is_attachable',
                    'Description': 'description',
                    'CreateDate': 'create_date',
                    'UpdateDate': 'update_date',
                    'PolicyVersionList': 'policy_version_list',
                }
            ],
        },
    },
}


def connect(connection_name, options):
    table_prefix = f'aws_collect' + (
        '' if connection_name in ('', 'default') else connection_name
    )
    table_name = f'{table_prefix}_organizations_list_accounts_connection'
    landing_table = f'{DATA_SCHEMA}.{table_name}'

    audit_assumer_arn = options['audit_assumer_arn']
    org_account_ids = options['org_account_ids']
    audit_reader_role = options['audit_reader_role']
    reader_eid = options.get('reader_eid', '')

    comment = yaml_dump(
        module='aws_collect',
        audit_assumer_arn=audit_assumer_arn,
        org_account_ids=org_account_ids,
        audit_reader_role=audit_reader_role,
        reader_eid=reader_eid,
        collect_apis='all',
    )

    db.create_table(name=landing_table, cols=LANDING_TABLE_COLUMNS, comment=comment)
    db.execute(f'GRANT INSERT, SELECT ON {landing_table} TO ROLE {SA_ROLE}')

    for table_postfix, cols in SUPPLEMENTARY_TABLES.items():
        supp_table = f'{DATA_SCHEMA}.{table_prefix}_{table_postfix}'
        db.create_table(name=supp_table, cols=cols)
        db.execute(f'GRANT INSERT, SELECT ON {supp_table} TO ROLE {SA_ROLE}')

    return {
        'newStage': 'finalized',
        'newMessage': "AWS Collect connector tables created.",
    }


def process_response_lists(coldict, page):
    for response_key, colname in coldict.items():
        response_value = page.get(response_key)

        if type(colname) is list:
            for x in response_value or []:
                yield process_response_items(colname[0], x, {})

        if type(colname) is dict:
            if response_value:
                yield from process_response_lists(colname, response_value)


def process_response_items(coldict, page, db_entry=None):
    if db_entry is None:
        db_entry = {}

    if type(coldict) is str:
        db_entry[coldict] = page

    elif type(coldict) is ParsedCol:
        parse: Any = PARSERS[coldict.type]
        db_entry[coldict.colname] = bytes_to_str(page)
        db_entry[coldict.parsed_colname] = parse(bytes_to_str(page))

    elif type(coldict) is dict:
        for response_key, colname in coldict.items():
            if page:  # e.g. sometimes get_login_profile returns None
                response_value = page.get(response_key)
                db_entry.update(process_response_items(colname, response_value))

    return db_entry


def process_aws_response(task, page):
    response_coldict = API_METHOD_SPECS[task.method]['response']
    children_list = API_METHOD_SPECS[task.method].get('children', [])
    params = API_METHOD_SPECS[task.method].get('params', {})

    base_entity = {}
    if task.account_id:
        base_entity['account_id'] = task.account_id

    base_entity.update({v: task.args[k] for k, v in params.items()})

    metadata = getattr(page, 'response', {}).get('ResponseMetadata', {})
    base_entity['recorded_at'] = (
        parse_date(metadata['HTTPHeaders']['date'])
        if 'HTTPHeaders' in metadata
        else datetime.now()
    )

    if isinstance(page, Exception):
        base_entity['error'] = {
            'message': format_exception_only(page),
            'exceptionName': page.__class__.__name__,
            'exceptionArgs': page.args,
            'exceptionTraceback': format_exception(page),
        }

        base_entity['error']['responseMetadata'] = metadata

        yield DBEntry(base_entity)
        return

    base_entity.update(process_response_items(response_coldict, page))

    iterated_entries = list(process_response_lists(response_coldict, page))

    for entry in iterated_entries or [base_entity]:
        db_entry = DBEntry(updated(base_entity.copy(), entry))
        yield db_entry
        for child in children_list:
            for method in child.get('methods', [child.get('method')]):
                request_args = child.get('args', {})
                required_args = child.get('required_args', [])
                if any(v not in db_entry.entity for v in request_args.values()):
                    continue
                if not all(db_entry.entity.get(k) for k in required_args):
                    continue
                args = {k: db_entry.entity[v] for k, v in request_args.items()}
                yield CollectTask(task.account_id, method, args)


async def load_task_response(client, task):
    async for x in metadata_rate_limit.iterate_with_retry(
        lambda: try_load_task_response(client, task),
        seconds_between_retries=1,
        exp_base=1.1,
    ):
        yield x


async def try_load_task_response(client, task):
    args = task.args or {}
    argspec = API_METHOD_SPECS[task.method].get('args', {})

    # e.g. for s3control.get_public_access_block
    if argspec.get('AccountId') == 'account_id':
        args['AccountId'] = task.account_id

    client_name, method_name = task.method.split('.', 1)

    try:
        if client.can_paginate(method_name):
            async for page in metadata_rate_limit.iterate_with_wait(
                client.get_paginator(method_name).paginate(**args)
            ):
                for x in process_aws_response(task, page):
                    yield x
        else:
            await metadata_rate_limit.wait(cost=1)
            for x in process_aws_response(
                task, await getattr(client, method_name)(**args)
            ):
                yield x

    except (ClientError, DataNotFoundError, ServerTimeoutError) as e:
        for x in process_aws_response(task, e):
            yield x


async def get_session(account_arn, client_name=None):
    # prune cache first to stop OOM errors
    in_10m = datetime.now(pytz.utc) + timedelta(minutes=10)
    for k, (expiration, _) in list(_SESSION_CACHE.items()):
        if expiration < in_10m:
            del _SESSION_CACHE[k]

    session_key = (account_arn, client_name)
    expiration, session = _SESSION_CACHE.get(session_key, (None, None))
    if expiration is None:
        await metadata_rate_limit.wait(cost=12)
        expiration, session = _SESSION_CACHE[
            session_key
        ] = await metadata_rate_limit.retry(
            lambda: aio_sts_assume_role(
                metadata_rate_limit,
                src_role_arn=AUDIT_ASSUMER_ARN,
                dest_role_arn=account_arn,
                dest_external_id=READER_EID,
            )
        )
    return session


async def process_task(task, add_task) -> AsyncGenerator[Tuple[str, dict], None]:
    account_arn = f'arn:{AWS_ZONE}:iam::{task.account_id}:role/{AUDIT_READER_ROLE}'
    account_info = {'account_id': task.account_id}

    client_name, method_name = task.method.split('.', 1)

    try:
        session = await get_session(account_arn)

        await metadata_rate_limit.wait()
        async with session.client(client_name, config=AIO_CONFIG) as client:
            if hasattr(client, 'describe_regions'):
                await metadata_rate_limit.wait()
                response = await client.describe_regions()
                region_names = [region['RegionName'] for region in response['Regions']]
            else:
                region_names = API_METHOD_SPECS[task.method].get('regions', [None])

        for rn in region_names:
            session = await get_session(account_arn, client_name)

            await metadata_rate_limit.wait()
            async with session.client(
                client_name, region_name=rn, config=AIO_CONFIG
            ) as client:
                async for response in load_task_response(client, task):
                    if type(response) is DBEntry:
                        if rn is not None:
                            response.entity['region'] = rn
                        yield (task.method, response.entity)
                    elif type(response) is CollectTask:
                        add_task(response)
                    else:
                        log.info('log response', response)

    except ClientError as e:
        # record missing auditor role as empty account summary
        yield (
            task.method,
            updated(
                account_info,
                recorded_at=parse_date(
                    e.response['ResponseMetadata']['HTTPHeaders']['date']
                ),
                error={
                    'message': format_exception_only(e),
                    'exceptionName': e.__class__.__name__,
                    'exceptionArgs': e.args,
                    'exceptionTraceback': format_exception(e),
                },
            ),
        )


def insert_list(name, values, table_name=None, dryrun=False):
    name = name.replace('.', '_')
    table_name = table_name or f'{DATA_SCHEMA}.aws_collect_{name}'
    log.info(f'inserting {len(values)} values into {table_name}')
    return db.insert(table_name, values, dryrun=dryrun)


async def aws_collect_task(task, wait=0.0, add_task=None):
    if wait:
        await asyncio.sleep(wait)

    # log.info(f'processing {task}')
    result_lists = defaultdict(list)
    async for k, v in process_task(task, add_task):
        result_lists[k].append(v)
    return result_lists


async def aioingest(table_name, options, dryrun=False):
    global AUDIT_ASSUMER_ARN
    global AUDIT_READER_ROLE
    global READER_EID
    global AWS_ZONE

    AUDIT_ASSUMER_ARN = options.get('audit_assumer_arn', AUDIT_ASSUMER_ARN)
    AUDIT_READER_ROLE = options.get('audit_reader_role', AUDIT_READER_ROLE)
    READER_EID = options.get('reader_eid', READER_EID)
    AWS_ZONE = options.get('aws_zone', AWS_ZONE)

    collect_apis = (
        [
            'iam.generate_credential_report',
            'iam.list_account_aliases',
            'iam.get_account_summary',
            'iam.get_account_password_policy',
            'efs.describe_file_systems',
            'ec2.describe_instances',
            'ec2.describe_nat_gateways',
            'ec2.describe_route_tables',
            'ec2.describe_security_groups',
            'ec2.describe_network_interfaces',
            'config.describe_configuration_recorders',
            'kms.list_keys',
            'iam.list_policies',
            'iam.list_virtual_mfa_devices',
            's3.list_buckets',
            'cloudtrail.describe_trails',
            'iam.list_roles',
            'inspector.list_findings',
            'iam.list_groups',
            's3control.get_public_access_block',
            'iam.get_account_authorization_details',
            'iam.list_users',
            'iam.get_credential_report',
            'ec2.describe_snapshots',
        ]
        if options.get('collect_apis', 'all') == 'all'
        else options.get('collect_apis').split(',')
    )

    oids = options.get('org_account_ids', '')
    oids = (
        [oid.strip() for oid in oids.split(',')]
        if type(oids) is str
        else [str(oids)]
        if type(oids) is int
        else map(str, oids)
        if type(oids) in (list, tuple)
        else oids
    )
    num_entries = 0
    for oid in oids:
        master_reader_arn = (
            options.get('master_reader_arn')
            if oid == ''
            else f'arn:{AWS_ZONE}:iam::{oid}:role/{AUDIT_READER_ROLE}'
        )

        if master_reader_arn is None:
            log.error("error: set 'master_reader_arn' or 'org_account_ids'")

        await metadata_rate_limit.wait(cost=12)
        expiration, session = await metadata_rate_limit.retry(
            lambda: aio_sts_assume_role(
                metadata_rate_limit,
                src_role_arn=AUDIT_ASSUMER_ARN,
                dest_role_arn=master_reader_arn,
                dest_external_id=READER_EID,
            )
        )

        await metadata_rate_limit.wait()
        async with session.client('organizations') as org_client:
            accounts = [
                a.entity
                async for a in load_task_response(
                    org_client, CollectTask(None, 'organizations.list_accounts', {})
                )
            ]

        for a in accounts:
            # the response is a single datetime (error ocurred)
            # then we treat it as a non-org account, and scan it alone
            if 'id' not in a:
                a['id'] = oid

        insert_list(
            'organizations.list_accounts',
            accounts,
            table_name=f'{DATA_SCHEMA}.{table_name}',
            dryrun=dryrun,
        )
        num_entries += len(accounts)

        collection_tasks = [
            CollectTask(a['id'], method, {})
            for method in collect_apis
            for a in accounts
        ]

        def add_task(t):
            collection_tasks.append(t)

        while collection_tasks:
            coroutines = [
                aws_collect_task(
                    t, wait=(i / _REQUEST_PACE_PER_SECOND), add_task=add_task
                )
                for i, t in enumerate(collection_tasks[:_REQUEST_BATCH_SIZE])
            ]
            del collection_tasks[:_REQUEST_BATCH_SIZE]
            log.info(
                f'progress: starting {len(coroutines)}, queued {len(collection_tasks)}'
            )

            all_results: Any = defaultdict(list)
            for coro in asyncio.as_completed(coroutines):
                result_lists = await coro
                for k, vs in result_lists.items():
                    all_results[k] += vs
            for name, vs in all_results.items():
                response = insert_list(name, vs, dryrun=dryrun)
                num_entries += len(vs)
                log.info(f'finished {name} {response}')

    return num_entries


def ingest(table_name, options, dryrun=False):
    return asyncio.get_event_loop().run_until_complete(
        aioingest(table_name, options, dryrun=dryrun)
    )


def main(
    table_name,
    audit_assumer_arn,
    reader_eid,
    audit_reader_role,
    collect_apis,
    master_reader_arn='',
    org_account_ids='',
    dryrun=False,
):
    ingest(
        table_name,
        {
            'audit_assumer_arn': audit_assumer_arn,
            'master_reader_arn': master_reader_arn,
            'org_account_ids': org_account_ids,
            'reader_eid': reader_eid,
            'audit_reader_role': audit_reader_role,
            'collect_apis': collect_apis,
        },
        dryrun=dryrun,
    )


if __name__ == '__main__':
    fire.Fire(main)
