import json
import pytest

from runners import violation_queries_runner, violation_suppressions_runner
from runners.helpers import db

TEST_QUERY = f"""
CREATE OR REPLACE VIEW rules._test1_violation_query COPY GRANTS
  COMMENT='Test Violation Summary
  @id test-violation-query-id
  @tags test-tag, test-violation-tag'
AS
SELECT 'SnowAlert Test Runner' AS environment
     , 'Test Violation Object' AS object
     , 'Test Violation Title' AS title
     , NULL AS alert_time
     , 'This is a testing violation.' AS description
     , OBJECT_CONSTRUCT('b', 1, 'a', 2) AS event_data
     , 'snowalert-test-detector' AS detector
     , 'low' AS severity
     -- , 'test-missing-owner' AS owner
     , 'test-violation-query-id' AS query_id
FROM (SELECT 1 AS test_data)
WHERE 1=1
  AND test_data=1
"""

TEST_INVALID_QUERY = f"""
CREATE OR REPLACE VIEW rules._test2_invalid_violation_query COPY GRANTS
  COMMENT='Test Invalid Violation Summary
  @id test-invalid-violation-query-id
  @tags test-invalid-violation-tag'
AS
SELECT NULL AS environment
     , NULL AS object
     , NULL AS title
     , CURRENT_TIMESTAMP() AS alert_time
     , NULL AS description
     , 1/0 AS event_data
     , NULL AS detector
     , NULL AS severity
     , NULL AS owner
     , 'test-violation-query-id' AS query_id
FROM (SELECT 1 AS test_data)
WHERE 1=1
  AND test_data=1
"""

TEST_SUPPRESSION = f"""
CREATE OR REPLACE VIEW rules._test1_violation_suppression COPY GRANTS
  COMMENT='Test Violation Suppression'
AS
SELECT id
FROM data.violations
WHERE suppressed IS NULL
  AND query_id='test-violation-query-id'
;
"""

TEARDOWN_QUERIES = [
    f"DROP VIEW IF EXISTS rules._test1_violation_query",
    f"DROP VIEW IF EXISTS rules._test1_violation_suppression",
    f"DROP VIEW IF EXISTS rules._test2_invalid_violation_query",
    f"DELETE FROM results.violations",
    f"DELETE FROM results.run_metadata",
    f"DELETE FROM results.query_metadata",
]


def json_like_snowflake(o) -> str:
    return json.dumps(o, separators=(',', ':'), sort_keys=True)


def json_like_connector(o) -> str:
    return json.dumps(o, indent=2, sort_keys=True)


@pytest.fixture
def violation_queries(db_schemas):
    db.execute(TEST_QUERY)
    db.execute(TEST_SUPPRESSION)
    db.execute(TEST_INVALID_QUERY)
    db.execute(
        f"""
        CREATE OR REPLACE VIEW rules.__suppress_samples_violation_suppression COPY GRANTS
          COMMENT='this should suppress anything not a test'
        AS
        SELECT id
        FROM data.violations
        WHERE suppressed IS NULL
          AND query_name NOT ILIKE '_TEST%'
    """
    )

    yield

    db.execute(f"DROP VIEW rules.__suppress_sample_alerts_alert_suppression")
    for q in TEARDOWN_QUERIES:
        db.execute(q)


def test_violation_tags_in_rule_tags_view(violation_queries):
    test_violation_tag_row = next(
        db.fetch("SELECT * FROM data.rule_tags WHERE tag='test-violation-tag'")
    )
    assert test_violation_tag_row == {
        'TYPE': 'QUERY',
        'TARGET': 'VIOLATION',
        'RULE_NAME': '_TEST1_VIOLATION_QUERY',
        'RULE_ID': 'test-violation-query-id',
        'TAG': 'test-violation-tag',
    }


def test_run_violations(violation_queries):
    from hashlib import md5

    #
    # run queries
    #

    violation_queries_runner.main()
    violations = list(db.fetch('SELECT * FROM data.violations ORDER BY query_name'))

    assert len(violations) == 2
    v = violations[-1]

    default_identity = {
        'ENVIRONMENT': "SnowAlert Test Runner",
        'OBJECT': v['OBJECT'],
        'TITLE': "Test Violation Title",
        'ALERT_TIME': None,
        'DESCRIPTION': "This is a testing violation.",
        'EVENT_DATA': {'b': 1, 'a': 2},
        'DETECTOR': "snowalert-test-detector",
        'SEVERITY': "low",
        'QUERY_ID': "test-violation-query-id",
        'QUERY_NAME': "_TEST1_VIOLATION_QUERY",
        'OWNER': None,
    }

    # basics
    assert (
        v['ID']
        == md5(json_like_snowflake(default_identity).encode('utf-8')).hexdigest()
    )
    assert v['OBJECT'] == "Test Violation Object"
    assert v['EVENT_DATA'] == {"b": 1, "a": 2}
    assert v['SUPPRESSED'] is None
    assert v['VIOLATION_TIME'] is None
    assert v['CREATED_TIME'] is not None

    # metadata
    queries_run_records = list(
        db.fetch('SELECT * FROM data.violation_queries_runs ORDER BY start_time')
    )
    assert len(queries_run_records) == 1
    assert queries_run_records[0]['NUM_VIOLATIONS_CREATED'] == 2

    query_rule_run_record = list(
        db.fetch('SELECT * FROM data.violation_query_rule_runs ORDER BY start_time')
    )
    assert len(query_rule_run_record) == 3
    assert query_rule_run_record[-2]['QUERY_NAME'] == '_TEST1_VIOLATION_QUERY'
    assert query_rule_run_record[-2]['NUM_VIOLATIONS_CREATED'] == 1
    assert query_rule_run_record[-1]['QUERY_NAME'] == '_TEST2_INVALID_VIOLATION_QUERY'
    assert query_rule_run_record[-1]['NUM_VIOLATIONS_CREATED'] == 0

    error = query_rule_run_record[-1].get('ERROR')
    assert type(error) is dict
    assert error['PROGRAMMING_ERROR'] == '100051 (22012): Division by zero'
    assert 'snowflake.connector.errors.ProgrammingError' in error['EXCEPTION_ONLY']
    assert 'Traceback (most recent call last)' in error['EXCEPTION']

    #
    # run supperessions
    #

    violation_suppressions_runner.main()
    vs = list(db.fetch('SELECT * FROM data.violations ORDER BY created_time'))
    v = vs[-1]

    # basics
    assert v['SUPPRESSED'] is True
    assert v['SUPPRESSION_RULE'] == '_TEST1_VIOLATION_SUPPRESSION'

    # metadata
    suppressions_run_record = next(
        db.fetch('SELECT * FROM data.violation_suppressions_runs')
    )
    assert suppressions_run_record['NUM_VIOLATIONS_PASSED'] == 0
    assert suppressions_run_record['NUM_VIOLATIONS_SUPPRESSED'] == 2

    get_runs = 'SELECT * FROM data.violation_suppression_rule_runs ORDER BY start_time'
    suppression_rule_run_record = list(db.fetch(get_runs))
    assert len(suppression_rule_run_record) == 2
    assert suppression_rule_run_record[0]['RULE_NAME'] == '_TEST1_VIOLATION_SUPPRESSION'
    assert suppression_rule_run_record[0]['NUM_VIOLATIONS_SUPPRESSED'] == 1
    assert (
        suppression_rule_run_record[1]['RULE_NAME']
        == '__SUPPRESS_SAMPLES_VIOLATION_SUPPRESSION'
    )
    assert suppression_rule_run_record[1]['NUM_VIOLATIONS_SUPPRESSED'] == 1


def test_schemas(db_schemas):
    assert next(db.fetch('SELECT 1 AS "a"')) == {'a': 1}
    assert list(db.fetch('SELECT * FROM results.violations')) == []
