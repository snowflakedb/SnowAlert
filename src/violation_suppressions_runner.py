#!/usr/bin/env python

from config import VIOLATIONS_TABLE, RULES_SCHEMA, VIOLATION_SQUELCH_POSTFIX, CLOUDWATCH_METRICS
from helpers import log
from helpers.db import connect, load_rules


def flag_remaining_alerts(ctx):
    try:
        ctx.cursor().execute(f"UPDATE {VIOLATIONS_TABLE} SET suppressed=FALSE WHERE suppressed IS NULL;")
    except Exception as e:
        log.fatal("Failed to flag remaining alerts as unsuppressed", e)


def run_suppression(squelch_name):
    ctx = connect()
    print(f"Received suppression {squelch_name}")
    try:
        ctx.cursor().execute(f"""
            MERGE INTO {VIOLATIONS_TABLE} t
            USING(SELECT result:EVENT_HASH AS event_hash FROM {RULES_SCHEMA}.{squelch_name}) s
            ON t.result:EVENT_HASH=s.event_hash
            WHEN MATCHED THEN UPDATE
            SET t.suppressed='true', t.suppression_rule='{squelch_name}';
        """)
    except Exception as e:
        log.fatal("Suppression query {squelch_name} execution failed.", e)

    print(f"Suppression query {squelch_name} executed")


def main():
    ctx = connect()
    for squelch_name in load_rules(ctx, VIOLATION_SQUELCH_POSTFIX):
        run_suppression(squelch_name)
    flag_remaining_alerts(ctx)

    if {CLOUDWATCH_METRICS}:
        log.metric('Run', 'SnowAlert', [{'Name': 'Component', 'Value': 'Violation Suppression Runner'}], 1)


if __name__ == '__main__':
    main()
