from runners import alert_suppressions_runner
from runners.helpers import db
import pytest
import os

CTX = db.connect()


def preprocess():
    alert_suppressions_runner.main()


def suppression_test_1():
    # Tests that a row in the alerts table is created when you run a query

    query = f"select * from results.alerts where alert:QUERY_ID = 'test_query_2'"
    rows = db.fetch(CTX, query)
    alerts = list(rows)
    assert len(alerts) == 1

    columns = alerts[0]
    assert columns['SUPPRESSED']
    assert columns['SUPPRESSION_RULE'] == 'TEST_2_ALERT_SUPPRESSION'


@pytest.mark.run(order=2)
def test():
    try:
        if os.environ['TEST_ENV'] == 'True':
            preprocess()
            suppression_test_1()
    except Exception:
        assert 1 == 0


if __name__ == '__main__':
    test()
