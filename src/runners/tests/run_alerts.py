import pytest
from unittest.mock import MagicMock

from runners.config import RUN_ID
from runners.helpers import db

TEST_ALERT = f"""
CREATE OR REPLACE VIEW rules._test1_alert_query COPY GRANTS
  COMMENT='Test 1 Alert Query; should group with Test 3
  @tags test-tag'
AS
SELECT OBJECT_CONSTRUCT('account', 'account_test', 'cloud', 'cloud_test') AS environment
    , ARRAY_CONSTRUCT('obj1', 'obj2') AS object
    , 'test1_alert_query_title' AS title
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

TEST_SUPPRESSED_ALERT = f"""
CREATE OR REPLACE VIEW rules._test2_alert_query COPY GRANTS
  COMMENT='Suppressed Alert Query
  @tags test-tag'
AS
SELECT OBJECT_CONSTRUCT('account', 'account_test', 'cloud', 'cloud_test') AS environment
    , ARRAY_CONSTRUCT('obj1', 'obj2') AS object
    , 'Suppressed Alert Query' AS title
    , 'This query should get suppressed' AS description
    , 'SnowAlert' AS detector
    , 'Test Actor' AS actor
    , 'Suppressed Alert Query Action' AS action
    , 'suppressed_alert_query_id' AS query_id
    , 'low' AS severity
    , ARRAY_CONSTRUCT('source') AS sources
    , OBJECT_CONSTRUCT('data', 'test 2 data') AS event_data
    , CURRENT_TIMESTAMP() AS event_time
    , CURRENT_TIMESTAMP() AS alert_time
FROM (
    SELECT 1 AS test_data
    UNION ALL
    SELECT 1 AS test_data
)
WHERE 1=1
  AND test_data=1
"""

TEST_SUPPRESSION = f"""
CREATE OR REPLACE VIEW rules._test2_alert_suppression COPY GRANTS
  COMMENT='test suppression; should suppress test 2'
AS
SELECT id
FROM data.alerts
WHERE suppressed IS NULL
  AND actor='Test Actor'
  AND action='Suppressed Alert Query Action'
"""

TEST_CORRELATED_ALERT = f"""
CREATE OR REPLACE VIEW rules._test3_alert_query COPY GRANTS
  COMMENT='Test 3 Alert Query; should group with Test 1
  @id test_3_query
  @tags test-tag'
AS
SELECT OBJECT_CONSTRUCT('account', 'account_test', 'cloud', 'cloud_test') AS environment
    , ARRAY_CONSTRUCT('obj1', 'obj2') AS object
    , 'test3_alert_query' AS title
    , 'This is a third test alert query; this should be grouped with Test 1' AS description
    , 'SnowAlert' AS detector
    , 'Common Test Actor' AS actor
    , 'test action 3' AS action
    , 'test_3_query' AS query_id
    , 'low' AS severity
    , ARRAY_CONSTRUCT('source') AS sources
    , OBJECT_CONSTRUCT('data', 'test 3 data') AS event_data
    , CURRENT_TIMESTAMP() AS event_time
    , CURRENT_TIMESTAMP() AS alert_time
FROM (SELECT 1 AS test_data)
WHERE 1=1
  AND test_data=1
"""

TEST_ALERT_WITH_SLACK_HANDLER = f"""
CREATE OR REPLACE VIEW rules._test4_alert_query COPY GRANTS
  COMMENT='Test 4 Alert Query; should call slack handler
  @tags test-tag'
AS
SELECT OBJECT_CONSTRUCT('account', 'account_test', 'cloud', 'cloud_test') AS environment
    , ARRAY_CONSTRUCT('obj1', 'obj2') AS object
    , 'test4_alert_query' AS title
    , 'This is a test alert query; this should call the slack handler' AS description
    , 'SnowAlert' AS detector
    , 'Slack Test Actor' AS actor
    , 'test action 4' AS action
    , 'test_4_query_id' AS query_id
    , 'low' AS severity
    , ARRAY_CONSTRUCT('source') AS sources
    , OBJECT_CONSTRUCT('data', 'test data') AS event_data
    , CURRENT_TIMESTAMP() AS event_time
    , CURRENT_TIMESTAMP() AS alert_time
    , ARRAY_CONSTRUCT(
        OBJECT_CONSTRUCT(
           'type', 'slack',
           'channel', 'seceng',
           'message', 'This is a test alert; the actor is ' || actor || ' and the action is ' || action || '.'
        )
      ) AS handlers
FROM (SELECT 1 AS test_data)
WHERE 1=1
  AND test_data=1
"""


EXPECTED_TEST_1_OUTPUT = {
    "ACTION": "test action 1",
    "ACTOR": "Common Test Actor",
    "DESCRIPTION": "This is a test alert query; this should be grouped with Test 3",
    "DETECTOR": "SnowAlert",
    "ENVIRONMENT": {"account": "account_test", "cloud": "cloud_test"},
    "EVENT_DATA": {"data": "test data"},
    "OBJECT": '["obj1","obj2"]',
    "QUERY_ID": "test_1_query_id",
    "QUERY_NAME": "_TEST1_ALERT_QUERY",
    "SEVERITY": "low",
    "SOURCES": ["source"],
    "TITLE": "test1_alert_query_title",
    "TICKET": None,
}

SLACK_MOCK_RETURN_VALUE = {'ok': True}


@pytest.fixture
def sample_alert_rules(db_schemas):
    db.execute(TEST_ALERT)
    db.execute(TEST_SUPPRESSED_ALERT)
    db.execute(TEST_SUPPRESSION)
    db.execute(TEST_CORRELATED_ALERT)
    db.execute(TEST_ALERT_WITH_SLACK_HANDLER)
    db.execute(
        f"""
        CREATE OR REPLACE VIEW rules.__suppress_sample_alerts_alert_suppression COPY GRANTS
          COMMENT='this should suppress anything not a test alert'
        AS
        SELECT id
        FROM data.alerts
        WHERE suppressed IS NULL
          AND query_name NOT ILIKE '_TEST%'
    """
    )

    yield

    db.execute(f"DROP VIEW rules._test1_alert_query")
    db.execute(f"DROP VIEW rules._test2_alert_query")
    db.execute(f"DROP VIEW rules._test2_alert_suppression")
    db.execute(f"DROP VIEW rules._test3_alert_query")
    db.execute(f"DROP VIEW rules._test4_alert_query")
    db.execute(f"DROP VIEW rules.__suppress_sample_alerts_alert_suppression")


@pytest.fixture
def update_jira_issue_status_done(request):
    issues_to_update = []

    @request.addfinalizer
    def fin():
        from runners.handlers import jira

        for jira_id in issues_to_update:
            jira.set_issue_done(jira_id)

    def mark_done(jira_id):
        issues_to_update.append(jira_id)

    yield mark_done


def assert_dict_is_subset(a, b):
    for x in a:
        assert x in b
        assert a[x] == b[x]


def assert_dict_has_subset(a, b):
    for x in b:
        assert x in a
        assert b[x] == a[x]


def test_alert_runners_processor_and_dispatcher(
    sample_alert_rules, update_jira_issue_status_done, delete_results
):

    #
    # queries runner
    #

    from runners import alert_queries_runner

    alert_queries_runner.main()

    # basics
    alert = next(db.get_alerts(query_id='test_1_query_id'))
    assert_dict_is_subset(EXPECTED_TEST_1_OUTPUT, alert)

    query_rule_run_record = list(
        db.fetch('SELECT * FROM data.alert_query_rule_runs ORDER BY query_name')
    )

    assert len(query_rule_run_record) == 7  # 3 from samples + 4 test alert queries

    assert query_rule_run_record[0]['QUERY_NAME'] == 'ACTIVITY_BY_ADMIN_ALERT_QUERY'
    queries_by_admin = 57
    assert query_rule_run_record[0]['NUM_ALERTS_CREATED'] == queries_by_admin

    assert (
        query_rule_run_record[1]['QUERY_NAME']
        == 'SNOWFLAKE_LOGIN_WITHOUT_MFA_ALERT_QUERY'
    )
    assert query_rule_run_record[1]['NUM_ALERTS_CREATED'] == 1

    assert (
        query_rule_run_record[2]['QUERY_NAME']
        == 'SNOWFLAKE_RESOURCE_CREATION_ALERT_QUERY'
    )
    # unclear why this value is non-deterministic in test suite
    resource_creations = query_rule_run_record[2]['NUM_ALERTS_CREATED']
    assert resource_creations >= 40

    assert query_rule_run_record[-4]['QUERY_NAME'] == '_TEST1_ALERT_QUERY'
    assert query_rule_run_record[-4]['NUM_ALERTS_CREATED'] == 1

    assert query_rule_run_record[-3]['QUERY_NAME'] == '_TEST2_ALERT_QUERY'
    assert (
        query_rule_run_record[-3]['NUM_ALERTS_CREATED'] == 1
    )  # second is de-duplicated

    assert query_rule_run_record[-2]['QUERY_NAME'] == '_TEST3_ALERT_QUERY'
    assert query_rule_run_record[-2]['NUM_ALERTS_CREATED'] == 1

    assert query_rule_run_record[-1]['QUERY_NAME'] == '_TEST4_ALERT_QUERY'
    assert query_rule_run_record[-1]['NUM_ALERTS_CREATED'] == 1

    print(query_rule_run_record)

    queries_run_records = list(
        db.fetch('SELECT * FROM data.alert_queries_runs ORDER BY start_time')
    )
    assert len(queries_run_records) == 1
    assert (
        queries_run_records[0]['NUM_ALERTS_CREATED']
        == resource_creations + queries_by_admin + 5
    )
    assert queries_run_records[0]['NUM_ALERTS_UPDATED'] == 0

    # TODO: errors
    # error = query_rule_run_record[3].get('ERROR')
    # assert type(error) is dict
    # assert error['PROGRAMMING_ERROR'] == '100051 (22012): Division by zero'
    # assert 'snowflake.connector.errors.ProgrammingError' in error['EXCEPTION_ONLY']
    # assert 'Traceback (most recent call last)' in error['EXCEPTION']

    #
    # suppressions runner
    #

    from runners import alert_suppressions_runner

    alert_suppressions_runner.main()

    # basics
    alerts = list(db.get_alerts(query_id='suppressed_alert_query_id'))
    assert len(alerts) == 1
    assert alerts[0]['SUPPRESSED'] is True
    assert alerts[0]['TICKET'] is None
    assert alerts[0]['SUPPRESSION_RULE'] == '_TEST2_ALERT_SUPPRESSION'

    suppression_rule_run_records = list(
        db.fetch('SELECT * FROM data.alert_suppression_rule_runs ORDER BY rule_name')
    )
    assert len(suppression_rule_run_records) == 3
    assert suppression_rule_run_records[0]['RUN_ID'] == RUN_ID
    assert (
        suppression_rule_run_records[0]['RULE_NAME']
        == 'SINGLE_FACTOR_EXCEPTIONS_ALERT_SUPPRESSION'
    )
    assert suppression_rule_run_records[0]['NUM_ALERTS_SUPPRESSED'] == 0
    assert suppression_rule_run_records[1]['RUN_ID'] == RUN_ID
    assert suppression_rule_run_records[1]['RULE_NAME'] == '_TEST2_ALERT_SUPPRESSION'
    assert suppression_rule_run_records[1]['NUM_ALERTS_SUPPRESSED'] == 1
    assert suppression_rule_run_records[2]['RUN_ID'] == RUN_ID
    assert (
        suppression_rule_run_records[2]['RULE_NAME']
        == '__SUPPRESS_SAMPLE_ALERTS_ALERT_SUPPRESSION'
    )
    assert (
        suppression_rule_run_records[2]['NUM_ALERTS_SUPPRESSED']
        == resource_creations + queries_by_admin + 1
    )

    suppression_run_records = list(
        db.fetch('SELECT * FROM data.alert_suppressions_runs')
    )
    assert len(suppression_run_records) == 1
    assert_dict_has_subset(
        suppression_run_records[0],
        {
            'RUN_ID': RUN_ID,
            'NUM_ALERTS_SUPPRESSED': resource_creations + queries_by_admin + 2,
            'NUM_ALERTS_PASSED': 3,
        },
    )

    # TODO: errors

    #
    # processor
    #

    from runners import alert_processor

    alert_processor.main()

    # basics
    rows = list(db.get_alerts(actor='Common Test Actor'))
    assert len(rows) == 2
    assert rows[0]['CORRELATION_ID'] is not None
    assert rows[0]['CORRELATION_ID'] != ""
    assert rows[1]['CORRELATION_ID'] is not None
    assert rows[1]['CORRELATION_ID'] != ""
    assert rows[0]['CORRELATION_ID'] == rows[1]['CORRELATION_ID']

    #
    # alert handler
    #
    from runners.handlers import slack

    slack.handle = MagicMock(return_value={'ok': True})
    from runners import alert_dispatcher
    from runners.handlers import jira

    alert_dispatcher.main()

    # jira
    ticket_id = next(db.get_alerts(query_id='test_1_query_id'))['TICKET']
    assert ticket_id is not None
    update_jira_issue_status_done(ticket_id)
    ticket_body = jira.get_ticket_description(ticket_id)
    lines = ticket_body.split('\n')
    assert lines[20] == '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
    assert {lines[2], lines[23]} == {
        'Query ID: test_1_query_id',
        'Query ID: test_3_query',
    }

    # slack
    alert = next(db.get_alerts(query_id='test_4_query_id'))
    assert alert['HANDLED'] == [{"success": True, "details": SLACK_MOCK_RETURN_VALUE}]
    slack.handle.assert_called_once()
