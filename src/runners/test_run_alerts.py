from runners import alert_queries_runner
from runners import alert_suppressions_runner
from runners import alert_processor
from runners import alert_handler
from runners import test_queries
from runners.plugins import create_jira
from runners.helpers import db
import json
import pytest

TEST_1_OUTPUT = {"ACTION": "test action 1",
                 "ACTOR": "test_actor",
                 "DESCRIPTION": "This is a test alert query; this should be grouped with Test 3",
                 "DETECTOR": "SnowAlert",
                 "ENVIRONMENT": {"account": "account_test", "cloud": "cloud_test"},
                 "EVENT_DATA": {"data": "test data"},
                 "OBJECT": ["obj1", "obj2"],
                 "QUERY_ID": "test_1_query",
                 "QUERY_NAME": "TEST1_ALERT_QUERY",
                 "SEVERITY": "low",
                 "SOURCES": ["source"],
                 "TITLE": "test1_alert_query"}

ALERT_QUERY_RUNNER_DATA = """
            select * from results.alerts
            where alert:QUERY_ID = 'test_1_query'
            order by alert_time desc
            limit 1
            """

ALERT_SUPPRESSION_RUNNER_DATA = "select * from results.alerts where alert:QUERY_ID = 'test_2_query'"

ALERT_PROCESSOR_DATA = "select * from results.alerts where alert:ACTOR = 'test_actor' and suppressed = false"


@pytest.fixture
def bookkeeping():
    ctx = db.connect()
    db.execute(ctx, test_queries.TEST_1_ALERT)
    db.execute(ctx, test_queries.TEST_2_ALERT)
    db.execute(ctx, test_queries.TEST_2_SUPPRESSION)
    db.execute(ctx, test_queries.TEST_3_ALERT)

    yield

    ctx = db.connect()
    db.execute(ctx, 'truncate table results.alerts')


def test_alerts(bookkeeping):

    # Alert Queries Runner Tests

    alert_queries_runner.main()

    # Tests that a row in the alerts table is created when you run a query

    alert = json.loads(next(db.fetch(ALERT_QUERY_RUNNER_DATA))['ALERT'])

    # Tests that the row created in the alerts table matches what we expect to see.
    for k in TEST_1_OUTPUT:
        assert alert[k] == TEST_1_OUTPUT[k]

    # Alert Supression Runner Tests

    alert_suppressions_runner.main()

    alerts = list(db.fetch(ALERT_SUPPRESSION_RUNNER_DATA))

    # Tests that only one alert was suppressed
    assert len(alerts) == 1

    # Tests that the alert is properly marked as suppressed, and labled by the rule
    columns = alerts[0]
    assert columns['SUPPRESSED'] is True
    assert columns['SUPPRESSION_RULE'] == 'TEST2_ALERT_SUPPRESSION'

    # Alert Correlation Tests

    alert_processor.main()

    rows = list(db.fetch(ALERT_PROCESSOR_DATA))

    assert len(rows) == 2

    a1 = rows[0]
    a2 = rows[1]

    # Tests that the alerts selected have correlation IDs
    assert a1['CORRELATION_ID'] is not None
    assert a1['CORRELATION_ID'] != ""
    assert a2['CORRELATION_ID'] is not None
    assert a2['CORRELATION_ID'] != ""

    # Tests that the alerts are properly correlated with the same ID
    assert a1['CORRELATION_ID'] == a2['CORRELATION_ID']

    # Jira Creation Tests

    alert_handler.main()

    ticket_id = next(db.fetch(db.connect(), test_queries.TEST_4_TICKET_QUERY))['TICKET']

    # Tests that a ticket id is properly set
    assert ticket_id != 'None'
    ticket_body = create_jira.get_ticket_description(ticket_id)
    lines = ticket_body.split('\n')

    # Tests that the ticket format is what we expect, with events appended in the proper order.
    assert lines[2] == 'Query ID: test_1_query'
    assert lines[20] == '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
    assert lines[23] == 'Query ID: test_3_query'
