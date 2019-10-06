import getpass
import time
from jira import JIRA

JIRA_ACCOUNT = 'snowflakecomputing'
PROJECT_NAME = 'SA'

# Max 100 issues can be retrieved from API at a time.
MAX_RESULTS = 100


def connect_jira():
    print('Connecting to Jira...')
    username = input('Jira username: ')
    pwd = getpass.getpass('Jira password: ')
    try:
        jira_api = JIRA(
            f'https://{JIRA_ACCOUNT}.atlassian.net', basic_auth=(username, pwd)
        )
    except Exception as e:
        print('Error connecting to Jira!\n', e)

    return jira_api


def close_all_tickets(jira_api):
    jira_query = f'project = {PROJECT_NAME} AND resolution = Unresolved'

    issues = jira_api.search_issues(
        jql_str=jira_query, maxResults=MAX_RESULTS, validate_query=True
    )

    done_counter = 0
    t0 = time.time()

    while len(issues) > 0:
        for issue in issues:
            issue.fields.status = 'Done'
            done_counter = done_counter + 1
            seconds = round(time.time() - t0)
            jira_api.transition_issue(issue, 'done')
            print(
                "Issue: {0.key} transitioned to Done. Total completed: {1} Seconds elapsed: {2}".format(
                    issue, done_counter, seconds
                )
            )
        issues = jira_api.search_issues(
            jql_str=jira_query, maxResults=MAX_RESULTS, validate_query=True
        )


def main():
    print('Starting Jira Bulk Changes script...')
    jira_api = connect_jira()
    close_all_tickets(jira_api)
    print('Script execution completed.')


if __name__ == "__main__":
    main()
