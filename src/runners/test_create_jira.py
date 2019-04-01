from runners import test_queries
from runners.plugins import create_jira

from runners.helpers import db


def test_jira_ticket_creation():
    rows = db.fetch(db.connect(), test_queries.TEST_4_TICKET_QUERY)
    ticket_id = next(rows)['TICKET']
    assert ticket_id != 'None'
    ticket_body = create_jira.get_ticket_description(ticket_id)
    lines = ticket_body.split('\n')
    assert lines[2] == 'Query ID: test_1_query'
    assert lines[20] == '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'
    assert lines[23] == 'Query ID: test_3_query'
