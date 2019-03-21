from runners import alert_suppressions_runner
from runners.helpers import db
import os

CTX = db.connect()


def setup():
    alert_suppressions_runner.main()


def suppression_test_1():
    # Tests that a row in the alerts table is created when you run a query

    query = f"select * from snowalert.results.alerts where alert:QUERY_ID = 'test_query_2'"
    rows = db.fetch(CTX, query)
    ret = True
    l = list(rows)
    if len(l) > 1:
        print("Suppression test error: too many results returned")
        ret = False

    columns = l[0]
    if columns['SUPPRESSED'] is not True:
        print('Suppression Test 1 Failure: Alert is not suppressed')
        ret = False
    if columns['SUPPRESSION_RULE'] != 'TEST_2_ALERT_SUPPRESSION':
        print('Suppression Test 1 Failure: Alert was caught by incorrect suppression')
        ret = False

    return ret


def main():
    print("Running test")
    if os.environ['TEST_ENV'] != 'True':
        print("Not running in test env, exiting without testing")
        return None

    setup()

    if suppression_test_1() is True:
        print("Suppression Test 1 passed!")
    else:
        print("Suppression Test 1 failed; see logs for failures")


if __name__ == '__main__':
    main()
