from runners import alert_processor
from runners.helpers import db
import os

CTX = db.connect()


def setup():
    alert_processor.main()


def processor_test_1():
    query = "select * from snowalert.results.alerts where alert:ACTOR = 'ky_kiske' and suppressed = false"
    rows = list(db.fetch(CTX, query))

    ret = True

    a1 = rows[0]
    a2 = rows[1]

    if a1['CORRELATION_ID'] != a2['CORRELATION_ID']:
        ret = False
        print("Processor Test 1 Failed: A1 and A2 are not correlated")
        print(f"Alert 1: {a1}")
        print("\n~~~~\n")
        print(f"Alert 2: {a2}")

    return ret


def main():
    setup()
    processor_test_1()


if __name__ == '__main__':
    main()
