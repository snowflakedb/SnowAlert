#!/usr/bin/env python

import fire

from runners import ingest_runner, connectors_runner
from runners import baseline_runner

from runners import alert_queries_runner
from runners import alert_suppressions_runner
from runners import alert_processor
from runners import alert_dispatcher

from runners import violation_queries_runner
from runners import violation_suppressions_runner

from runners.config import RUN_ID
from runners.helpers import log


def main(target="all", rule_name=None):
    if target == "connector" and rule_name:
        connectors_runner.main(rule_name.upper())

    elif target == "processor":
        alert_processor.main()

    elif target == "dispatcher":
        alert_dispatcher.main()

    elif rule_name:
        if rule_name.endswith("_ALERT_QUERY"):
            alert_queries_runner.main(rule_name.upper())

        if rule_name.endswith("_ALERT_SUPPRESSION"):
            alert_suppressions_runner.main(rule_name.upper())

        if rule_name.endswith("_VIOLATION_QUERY"):
            violation_queries_runner.main(rule_name.upper())

        if rule_name.endswith("_VIOLATION_SUPPRESSION"):
            violation_suppressions_runner.main(rule_name.upper())

    else:
        log.info(f"STARTING RUN WITH ID {RUN_ID}")
        log.info(f"got command {target}")
        if target in ['alert', 'alerts', 'all']:
            alert_queries_runner.main()
            alert_suppressions_runner.main()
            alert_processor.main()
            alert_dispatcher.main()

        if target in ['violation', 'violations', 'all']:
            violation_queries_runner.main()
            violation_suppressions_runner.main()

        if target in ['ingest']:
            ingest_runner.main()
            connectors_runner.main()

        if target in ['connectors']:
            connectors_runner.main()

        if target in ['baseline', 'baselines']:
            baseline_runner.main()


if __name__ == '__main__':
    fire.Fire(main)
