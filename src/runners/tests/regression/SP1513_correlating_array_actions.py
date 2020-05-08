"""
Some of our alerts have arrays in the action field, which breaks correlations.
"""

import pytest

from runners.helpers import db


FIRST_ALERT_QUERY = f"""
CREATE OR REPLACE VIEW rules.first_alert_query COPY GRANTS
  COMMENT='Simple Alert Query creates one test alert
  @tags plain-test-tag'
AS
SELECT OBJECT_CONSTRUCT('account', 'account_test', 'cloud', 'cloud_test') AS environment
    , ARRAY_CONSTRUCT('obj1', 'obj2') AS object
    , 'first_alert_query' AS title
    , 'This is a simple alert query' AS description
    , 'SnowAlert' AS detector
    , 'Test Actor' AS actor
    , ARRAY_CONSTRUCT('act1', 'act2') AS action
    , 'test_query_id' AS query_id
    , 'low' AS severity
    , ARRAY_CONSTRUCT('source') AS sources
    , OBJECT_CONSTRUCT('data', 'test data') AS event_data
    , CURRENT_TIMESTAMP() AS event_time
    , CURRENT_TIMESTAMP() AS alert_time
FROM (SELECT 1 AS test_data)
WHERE 1=1
  AND test_data=1
"""

SECOND_ALERT_QUERY = f"""
CREATE OR REPLACE VIEW rules.second_alert_query COPY GRANTS
  COMMENT='Simple Alert Query creates one test alert
  @tags plain-test-tag'
AS
SELECT OBJECT_CONSTRUCT('account', 'account_test', 'cloud', 'cloud_test') AS environment
    , ARRAY_CONSTRUCT('obj1') AS object
    , 'second_alert_query' AS title
    , 'This is a simple alert query' AS description
    , 'SnowAlert' AS detector
    , 'Test Actor' AS actor
    , ARRAY_CONSTRUCT('act1', 'act2') AS action
    , 'test_query_id' AS query_id
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
def sample_alert_rules(db_schemas_no_samples):
    db.execute(FIRST_ALERT_QUERY)
    db.execute(SECOND_ALERT_QUERY)
    yield
    db.execute("DROP VIEW rules.first_alert_query")
    db.execute("DROP VIEW rules.second_alert_query")


def test_correlating_array_actions(
    sample_alert_rules, delete_results
):
    from runners import alert_queries_runner
    from runners import alert_suppressions_runner
    from runners import alert_processor

    alert_queries_runner.main()
    alert_suppressions_runner.main()
    alert_processor.main()

    rows = list(db.get_alerts(query_id='test_query_id', suppressed='false'))
    assert len(rows) == 2

    assert rows[0]['CORRELATION_ID'] == rows[1]['CORRELATION_ID']
