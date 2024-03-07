#!/usr/bin/env python

import getpass
from typing import Any

from fire import Fire
from jira import JIRA

from runners.handlers.jira import user, password

JIRA_ACCOUNT = 'snowflakecomputing'
PROJECT_NAME = 'SA'

# Max 100 issues can be retrieved from API at a time.
MAX_RESULTS = 100
CLIENT: Any = None


def connect_jira():
    global CLIENT
    try:
        CLIENT = JIRA(
            f'https://{JIRA_ACCOUNT}.atlassian.net',
            basic_auth=(
                user or input('Jira username: '),
                password or getpass.getpass('Jira password: '),
            ),
        )
    except Exception as e:
        print('Error connecting to Jira!\n', e)


def find_issues(query):
    return CLIENT.search_issues(
        jql_str=query, maxResults=MAX_RESULTS, validate_query=True
    )


def transition_issues_to_done(query):
    issues = find_issues(query)
    while len(issues) > 0:
        for issue in issues:
            issue.fields.status = 'Done'
            CLIENT.transition_issue(issue, 'done')
            print(f"{issue.key} -> Done.")
        issues = find_issues(query)


def main(query=f'project = {PROJECT_NAME} AND resolution = Unresolved'):
    connect_jira()
    transition_issues_to_done(query)


if __name__ == "__main__":
    Fire(main)
