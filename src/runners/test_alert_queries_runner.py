from runners import alert_queries_runner
from runners.helpers import db
import os
import json

CTX = db.connect()

TEST_1_OUTPUT = {"ACTION": "Standing K",
                 "ACTOR": "ky_kiske",
                 "DESCRIPTION": "Ky Kiske performed an unusual footsies maneuver: Standing K",
                 "DETECTOR": "SnowAlert",
                 "ENVIRONMENT": {"account": "REV", "cloud": "GG"},
                 "EVENT_DATA": {"Distance": "500 units",
                                "P1": "ky_kiske",
                                "P1_Input": "5K",
                                "P2": "sol_badguy",
                                "P2_Input": "5S"},
                 "OBJECT": ["^core", "XX^core+r"],
                 "QUERY_ID": "test_query_1",
                 "QUERY_NAME": "TEST_1_ALERT_QUERY",
                 "SEVERITY": "High",
                 "SOURCES": ["trails"],
                 "TITLE": "test_alert_query"}


def setup():
    db.execute(CTX, 'truncate table snowalert.results.alerts')
    alert_queries_runner.main()
    return None


def alert_test_1():
    # Tests that a row in the alerts table is created when you run a query
    query = """
            select * from snowalert.results.alerts
            where alert:QUERY_ID = 'test_query_1'
            order by alert_time desc
            limit 1
            """
    rows = db.fetch(CTX, query)
    row = next(rows)
    alert = json.loads(row['ALERT'])

    ret = True

    for k in TEST_1_OUTPUT:
        if alert[k] != TEST_1_OUTPUT[k]:
            print(f"TEST 1 Failure: Value Mismatch on {k}: {alert[k]}")
            ret = False

    return ret


def main():
    print("Running test")
    if os.environ['TEST_ENV'] != 'True':
        print("Not running in test env, exiting without testing")
        return None
    setup()

    if alert_test_1() is True:
        print("Alert Test 1 passed!")
    else:
        print("Alert Test 1 failed; see logs for failures")


if __name__ == '__main__':
    main()
