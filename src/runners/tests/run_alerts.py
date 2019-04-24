import pytest

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

TEST_SUPPRESSED_ALERT = f"""
CREATE OR REPLACE VIEW rules.test2_alert_query COPY GRANTS
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
CREATE OR REPLACE VIEW rules.test2_alert_suppression COPY GRANTS
  COMMENT='test suppression; should suppress test 2'
AS
SELECT id
FROM data.alerts
WHERE suppressed IS NULL
  AND actor='Test Actor'
  AND action='Suppressed Alert Query Action'
"""

TEST_CORRELATED_ALERT = f"""
CREATE OR REPLACE VIEW rules.test3_alert_query COPY GRANTS
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


EXPECTED_TEST_1_OUTPUT = {
    "ACTION": "test action 1",
    "ACTOR": "Common Test Actor",
    "DESCRIPTION": "This is a test alert query; this should be grouped with Test 3",
    "DETECTOR": "SnowAlert",
    "ENVIRONMENT": {"account": "account_test", "cloud": "cloud_test"},
    "EVENT_DATA": {"data": "test data"},
    "OBJECT": '["obj1","obj2"]',
    "QUERY_ID": "test_1_query_id",
    "QUERY_NAME": "TEST1_ALERT_QUERY",
    "SEVERITY": "low",
    "SOURCES": ["source"],
    "TITLE": "test1_alert_query",
    "TICKET": None,
}


@pytest.fixture
def sample_alert_rules(db_schemas):
    db.execute(TEST_ALERT)
    db.execute(TEST_SUPPRESSED_ALERT)
    db.execute(TEST_SUPPRESSION)
    db.execute(TEST_CORRELATED_ALERT)

    yield

    db.execute(f"DROP VIEW rules.test1_alert_query")
    db.execute(f"DROP VIEW rules.test2_alert_query")
    db.execute(f"DROP VIEW rules.test2_alert_suppression")
    db.execute(f"DROP VIEW rules.test3_alert_query")


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


def assert_dict_subset(a, b):
    for x in a:
        assert x in b
        assert a[x] == b[x]


def test_alert_runners_processor_and_jira_handler(sample_alert_rules, update_jira_issue_status_done, delete_results):

    #
    # queries runner
    #

    from runners import alert_queries_runner
    alert_queries_runner.main()

    # basics
    alert = next(db.get_alerts(query_id='test_1_query_id'))
    assert_dict_subset(EXPECTED_TEST_1_OUTPUT, alert)

    query_rule_run_record = list(db.fetch('SELECT * FROM data.alert_query_rule_runs ORDER BY query_name'))
    assert len(query_rule_run_record) == 3
    assert query_rule_run_record[0]['QUERY_NAME'] == 'TEST1_ALERT_QUERY'
    assert query_rule_run_record[0]['NUM_ALERTS_CREATED'] == 1

    assert query_rule_run_record[1]['QUERY_NAME'] == 'TEST2_ALERT_QUERY'
    assert query_rule_run_record[1]['NUM_ALERTS_CREATED'] == 1  # second is de-duplicated

    assert query_rule_run_record[2]['QUERY_NAME'] == 'TEST3_ALERT_QUERY'
    assert query_rule_run_record[2]['NUM_ALERTS_CREATED'] == 1

    queries_run_records = list(db.fetch('SELECT * FROM data.alert_queries_runs ORDER BY start_time'))
    assert len(queries_run_records) == 1
    assert queries_run_records[0]['NUM_ALERTS_CREATED'] == 3
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
    assert alerts[0]['SUPPRESSION_RULE'] == 'TEST2_ALERT_SUPPRESSION'

    suppression_run_records = list(db.fetch('SELECT * FROM data.alert_suppressions_runs'))
    assert len(suppression_run_records) == 1
    suppression_run_records[0]['NUM_ALERTS_SUPPRESSED'] = 1
    suppression_run_records[0]['NUM_ALERTS_PASSED'] = 2

    suppression_rule_run_records = list(db.fetch('SELECT * FROM data.alert_suppression_rule_runs'))
    assert len(suppression_rule_run_records) == 1
    suppression_rule_run_records[0]['NUM_ALERTS_SUPPRESSED'] = 1
    suppression_rule_run_records[0]['RULE_NAME'] = 'TEST2_ALERT_SUPPRESSION'

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
    # jira handler
    #

    from runners import alert_handler
    from runners.plugins import create_jira
    alert_handler.main()

    ticket_id = next(db.get_alerts(query_id='test_1_query_id'))['TICKET']
    assert ticket_id is not None
    update_jira_issue_status_done(ticket_id)
    ticket_body = create_jira.get_ticket_description(ticket_id)
    lines = ticket_body.split('\n')
    assert lines[20] == '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
    assert {lines[2], lines[23]} == {'Query ID: test_1_query_id', 'Query ID: test_3_query'}
