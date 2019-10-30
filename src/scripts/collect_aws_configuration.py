from dateutil.parser import parse as parse_date
import fire

from connectors.utils import sts_assume_role
from runners.helpers import db


AUDIT_ASSUMER = ''
MASTER_READER = ''
READER_EIDS = ''
AUDIT_READER_ROLE = 'audit-reader'

KEYS_TO_COLUMNS = {
    'users': {
        'AccountId': 'account_id',
        'ResponseHeaderDate': 'recorded_at',
        'Path': 'path',
        'UserName': 'user_name',
        'UserId': 'user_id',
        'Arn': 'arn',
        'CreateDate': 'create_date',
        'PasswordLastUsed': 'password_last_used',
    },
    'groups_for_user': {
        'AccountId': 'account_id',
        'ResponseHeaderDate': 'recorded_at',
        'Path': 'path',
        'GroupName': 'group_name',
        'GroupId': 'group_id',
        'Arn': 'arn',
        'CreateDate': 'create_date',
        'UserName': 'user_name',
    },
}


def updated(d, *ds, **kwargs):
    for new_d in ds:
        d.update(new_d)
    if kwargs:
        d.update(kwargs)
    return d


def aws_collect(client, entity, method=None, params=None):
    if method is None:
        method = 'list_' + entity.lower()

    if params is None:
        params = {}

    paginator = client.get_paginator(method)
    page_iterator = paginator.paginate(**params)
    for page in page_iterator:
        for x in page[entity.capitalize()]:
            x['ResponseHeaderDate'] = parse_date(
                page['ResponseMetadata']['HTTPHeaders']['date']
            )
            yield x


def list_groups_for_user(client, user_name):
    for group in aws_collect(
        client, 'groups', 'list_groups_for_user', {'UserName': user_name}
    ):
        group['UserName'] = user_name
        yield group


def load_aws_iam_lists(from_account_with_id):
    account_arn = f'arn:aws:iam::{from_account_with_id}:role/{AUDIT_READER_ROLE}'
    session = sts_assume_role(
        src_role_arn=AUDIT_ASSUMER,
        dest_role_arn=account_arn,
        dest_external_id=READER_EIDS,
    )
    iam = session.client('iam')

    account_info = {'AccountId': from_account_with_id}
    users = [updated(u, account_info) for u in aws_collect(iam, 'Users')]
    groups_for_user = [
        updated(group, account_info)
        for user in users
        for group in list_groups_for_user(iam, user['UserName'])
    ]

    return {'users': users, 'groups_for_user': groups_for_user} if session else None


def main(audit_assumer, master_reader, reader_eids, audit_reader_role):
    global AUDIT_ASSUMER
    global MASTER_READER
    global READER_EIDS
    global AUDIT_READER_ROLE
    AUDIT_ASSUMER = audit_assumer
    MASTER_READER = master_reader
    READER_EIDS = reader_eids
    AUDIT_READER_ROLE = audit_reader_role

    org_client = sts_assume_role(
        src_role_arn=AUDIT_ASSUMER,
        dest_role_arn=MASTER_READER,
        dest_external_id=READER_EIDS,
    ).client('organizations')

    account_pages = org_client.get_paginator('list_accounts').paginate()
    accounts = [
        account
        for page in account_pages
        for account in page['Accounts']
        if account['Name'] in ('sfc-bastion-dev', 'sfc-dev')
    ]
    for a in accounts:
        lists = load_aws_iam_lists(from_account_with_id=a['Id'])
        for list_name, values in lists.items():
            rows = [
                {KEYS_TO_COLUMNS[list_name][k]: v for k, v in value.items()}
                for value in values
            ]
            db.insert(f'data.aws_iam_list_{list_name}', rows)


if __name__ == '__main__':
    fire.Fire(main)
