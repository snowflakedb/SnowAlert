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
    ticket_id = next(rows)['TICKET']
    assert ticket_id != 'None'
    ticket_body = create_jira.get_ticket_description(ticket_id)
    lines = ticket_body.split('\n')
    assert lines[2] == 'Query ID: test_1_query'
    assert lines[20] == '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
    assert lines[23] == 'Query ID: test_3_query'


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
