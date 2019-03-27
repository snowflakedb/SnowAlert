from runners import alert_processor
from runners.helpers import db
import os
import pytest

CTX = db.connect()


def preprocess():
    alert_processor.main()


def processor_test_1():
    query = "select * from results.alerts where alert:ACTOR = 'test_actor' and suppressed = false"
    rows = list(db.fetch(CTX, query))

    assert len(rows) == 2

    a1 = rows[0]
    a2 = rows[1]

    assert len(a1['CORRELATION_ID']) > 5
    assert len(a2['CORRELATION_ID']) > 5
    assert a1['CORRELATION_ID'] == a2['CORRELATION_ID']


@pytest.mark.run(order=3)
def test():
    try:
        if os.environ['TEST_ENV'] == 'True':
            preprocess()
            processor_test_1()
    except Exception:
        assert 1 == 0


if __name__ == '__main__':
    test()
