from runners import alert_queries_runner
from runners import test_queries
from runners.helpers import db
import os
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


def preprocess():
    ctx = db.connect()
    assert ctx is not None
    db.execute(ctx, 'truncate table results.alerts')
    db.execute(ctx, test_queries.TEST_1_ALERT)
    db.execute(ctx, test_queries.TEST_2_ALERT)
    db.execute(ctx, test_queries.TEST_2_SUPPRESSION)
    db.execute(ctx, test_queries.TEST_3_ALERT)
    alert_queries_runner.main()
    return ctx


def alert_test_1(ctx):
    # Tests that a row in the alerts table is created when you run a query
    query = """
            select * from results.alerts
            where alert:QUERY_ID = 'test_1_query'
            order by alert_time desc
            limit 1
            """
    rows = db.fetch(ctx, query)
    row = next(rows)
    alert = json.loads(row['ALERT'])

    for k in TEST_1_OUTPUT:
        assert alert[k] == TEST_1_OUTPUT[k]


@pytest.mark.run(order=1)
def test():
    try:
        if os.environ['TEST_ENV'] == 'True':
            ctx = preprocess()
            alert_test_1(ctx)
    except Exception:
        assert 0


if __name__ == '__main__':
    test()
