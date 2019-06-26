'''SA Ingestion Runner (SAIR)

SAIR processes all the files in the src/ingestion folder
'''

import os
import subprocess

from runners.helpers import log


def main():
    for name in os.listdir('../ingestion'):
        log.info(f"--- Ingesting using {name}")
        try:
            res = subprocess.call(f"python ../ingestion/{name}", shell=True)
            log.info("subprocess returns: ", res)
            log.info(f"{name} invoked")
        except Exception as e:
            log.error(f"failed to run {name}", e)


if __name__ == "__main__":
    main()
