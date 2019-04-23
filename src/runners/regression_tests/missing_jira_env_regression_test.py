#
# Regression test for SP-1099
# If jira env variables are not declared, the alert handler should not enter the jira flow.
#

import pytest
import os
import sys

from runners.helpers import db

TEST_ALERT = f"""
CREATE OR REPLACE VIEW rules.test1_alert_query COPY GRANTS
  COMMENT='Test 1 Alert Query; should group with Test 3
  @tags test-tag'
AS
SELECT OBJECT_CONSTRUCT('account', 'account_test', 'cloud', 'cloud_test') AS environment
    , ARRAY_CONSTRUCT('obj1', 'obj2') AS object
    , 'test1_alert_query' AS title
    , 'This is a test alert query; this should be grouped with Test 3' AS description
    , 'SnowAlert' AS detector
    , 'Common Test Actor' AS actor
    , 'test action 1' AS action
    , 'test_1_query_id' AS query_id
    , 'low' AS severity
    , ARRAY_CONSTRUCT('source') AS sources
    , OBJECT_CONSTRUCT('data', 'test data') AS event_data
    , CURRENT_TIMESTAMP() AS event_time
    , CURRENT_TIMESTAMP() AS alert_time
FROM (SELECT 1 AS test_data)
WHERE 1=1
  AND test_data=1
"""


@pytest.fixture
def sample_alert_rules(db_schemas):
    db.execute(TEST_ALERT)

    yield

    # schemas will drop all that


@pytest.fixture
def update_jira_issue_status_done(request):
    issues_to_update = []

    @request.addfinalizer
    def fin():
        from runners.plugins import create_jira
        for jira_id in issues_to_update:
            create_jira.set_issue_done(jira_id)

    def mark_done(jira_id):
        issues_to_update.append(jira_id)

    yield mark_done


def test_missing_jira_env_regression(sample_alert_rules, update_jira_issue_status_done):
    # del os.environ['JIRA_PROJECT', 'JIRA_USER', 'JIRA_PASSWORD', 'JIRA_URL']
    sys.path.append('SnowAlert/src/runners')
    os.environ.pop('JIRA_PROJECT')
    os.environ.pop('JIRA_USER')
    os.environ.pop('JIRA_PASSWORD')
    os.environ.pop('JIRA_URL')
    assert 'JIRA_PROJECT' not in os.environ
    from runners import alert_queries_runner
    from runners import alert_suppressions_runner
    from runners import alert_processor
    from runners import alert_handler

    alert_queries_runner.main()
    alert_suppressions_runner.main()
    alert_processor.main()
    alert_handler.main()
    rows = list(db.get_alerts(suppressed='false'))
    assert rows[0]['TICKET'] == "None"
