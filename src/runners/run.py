#!/usr/bin/env python

import fire

from runners import ingest_runner

from runners import alert_queries_runner
from runners import alert_suppressions_runner
from runners import alert_processor
from runners import alert_handler

from runners import violation_queries_runner
from runners import violation_suppressions_runner

from runners.config import RUN_ID
from runners.helpers import log


def main(target="all", rule_name=None):
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
        log.info(f"STARTING RUN WITH ID {RUN_ID}")
        if target in ['alerts', 'all']:
            alert_queries_runner.main()
            alert_suppressions_runner.main()
            alert_processor.main()
            alert_handler.main()

        if target in ['violations', 'all']:
            violation_queries_runner.main()
            violation_suppressions_runner.main()

        if target in ['ingest']:
            ingest_runner.main()


if __name__ == '__main__':
    fire.Fire(main)
