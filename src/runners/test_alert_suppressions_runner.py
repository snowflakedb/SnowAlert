from runners.helpers import db


def test_2_query_suppression():
    # Tests that a row in the alerts table is created when you run a query

    query = f"select * from results.alerts where alert:QUERY_ID = 'test_2_query'"
    rows = db.fetch(db.connect(), query)
    alerts = list(rows)
    assert len(alerts) == 1

    columns = alerts[0]
    assert columns['SUPPRESSED']
    assert columns['SUPPRESSION_RULE'] == 'TEST2_ALERT_SUPPRESSION'
