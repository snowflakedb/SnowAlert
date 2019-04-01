from runners.helpers import db


def test_alert_correlations():
    query = "select * from results.alerts where alert:ACTOR = 'test_actor' and suppressed = false"
    rows = list(db.fetch(db.connect(), query))

    assert len(rows) == 2

    a1 = rows[0]
    a2 = rows[1]

    assert len(a1['CORRELATION_ID']) > 5
    assert len(a2['CORRELATION_ID']) > 5
    assert a1['CORRELATION_ID'] == a2['CORRELATION_ID']
