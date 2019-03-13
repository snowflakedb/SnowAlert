#!/usr/bin/env python

import os
import sys

from runners import alert_queries_runner
from runners import alert_suppressions_runner
from runners import alert_handler
from runners import violation_queries_runner
from runners import violation_suppressions_runner
from runners import alert_processor
from runners import ingest_runner


def main(command, rule_name=None):
    if rule_name:
        if rule_name.startswith("AQ."):
            alert_queries_runner.main(rule_name[3:].upper())
        if rule_name.startswith("AS."):
            alert_suppressions_runner.main(rule_name[3:].upper())
        if rule_name.startswith("AH."):
            alert_handler.main(rule_name[3:].upper())
        if rule_name.startswith("VQ."):
            violation_queries_runner.main(rule_name[3:].upper())
        if rule_name.startswith("VS."):
            violation_suppressions_runner.main(rule_name[3:].upper())
        alert_processor.main()
    else:
        if command in ['alerts', 'all']:
            alert_queries_runner.main()
            alert_suppressions_runner.main()
            alert_processor.main()
            if os.environ.get('JIRA_USER'):
                print("starting the jira handler, condition was true")
                alert_handler.main()
            else:
                print("No JIRA_USER in env, skipping handler.")

        if command in ['violations', 'all']:
            violation_queries_runner.main()
            violation_suppressions_runner.main()

        if command in ['ingest']:
            ingest_runner.main()


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] in ['alerts', 'violations', 'all']:
        main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
    else:
        print('usage: run.py [alerts|violations|all]', file=sys.stderr, flush=True)
