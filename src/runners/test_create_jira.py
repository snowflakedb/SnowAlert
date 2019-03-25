from runners import alert_handler
from runners import test_queries
from runners.helpers import db
from runners.plugins import create_jira
import pytest
import os

CTX = db.connect()


def preprocess():
    alert_handler.main()


def handler_test_1():
    rows = db.fetch(CTX, test_queries.TEST_4_TICKET_QUERY)
    row = rows.next()
    print(row)
    assert 1


@pytest.mark.run(order=4)
def test():
    try:
        if os.environ['TEST_ENV'] == 'True':
            preprocess()
            handler_test_1()
    except Exception:
        assert 1 == 0


if __name__ == '__main__':
    test()
