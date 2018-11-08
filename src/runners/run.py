#!/usr/bin/env python

import os
import sys

import alert_queries_runner
import alert_suppressions_runner
import alert_handler
import violation_queries_runner
import violation_suppressions_runner


def main(command):
    if command in ['alerts', 'all']:
        alert_queries_runner.main()
        alert_suppressions_runner.main()
        if os.environ.get('JIRA_USER'):
            alert_handler.main()
        else:
            print("No JIRA_USER in env, skipping handler.")

    if command in ['violations', 'all']:
        violation_queries_runner.main()
        violation_suppressions_runner.main()


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] in ['alerts', 'violations', 'all']:
        main(sys.argv[1])
    else:
        print('usage: run.py [alerts|violations|all]', file=sys.stderr, flush=True)
