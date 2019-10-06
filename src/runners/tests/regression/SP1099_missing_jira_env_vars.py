"""
If Jira env variables are not declared, the alert handler should not enter the
Jira flow, but should still run and complete the alert flow without errors.
"""

from importlib import reload
import pytest
from os import environ

from runners.helpers import db
from runners.handlers import jira


TEST_ALERT = f"""
CREATE OR REPLACE VIEW rules.simple_alert_query COPY GRANTS
  COMMENT='Simple Alert Query creates one test alert
  @tags plain-test-tag'
AS
SELECT OBJECT_CONSTRUCT('account', 'account_test', 'cloud', 'cloud_test') AS environment
    , ARRAY_CONSTRUCT('obj1', 'obj2') AS object
    , 'simple_alert_query' AS title
    , 'This is a simple alert query' AS description
    , 'SnowAlert' AS detector
    , 'Test Actor' AS actor
    , 'test action' AS action
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
def sample_alert_rule(db_schemas):
    db.execute(TEST_ALERT)
    yield
    db.execute("DROP VIEW rules.simple_alert_query")


@pytest.fixture
def without_jira_vars():
    backup = environ.copy()
    del environ['JIRA_PROJECT']
    del environ['JIRA_USER']
    del environ['JIRA_PASSWORD']
    del environ['JIRA_URL']
    reload(jira)
    yield
    environ.update(backup)
    reload(jira)


def test_missing_jira_env_regression(
    sample_alert_rule, without_jira_vars, delete_results
):
    from runners import alert_queries_runner
    from runners import alert_suppressions_runner
    from runners import alert_processor
    from runners import alert_dispatcher

    alert_queries_runner.main()
    alert_suppressions_runner.main()
    alert_processor.main()
    alert_dispatcher.main()

    rows = list(db.get_alerts(query_id='test_query_id', suppressed='false'))
    assert len(rows) == 1
    assert rows[0]['TICKET'] is None
