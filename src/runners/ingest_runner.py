'''SA Ingestion Runner (SAIR)

SAIR processes all the files in the src/ingestion folder
'''

import os
import subprocess

from runners.helpers import log


def main(script=None):
    scripts = [script] if script else os.listdir('../ingestion')

    for name in scripts:
        log.info(f"--- Ingesting using {name}")
        try:
            res = subprocess.call(f"python ../ingestion/{name}", shell=True)
            log.info("subprocess returns:", res)
        except Exception as e:
            log.error(f"exception raised:", e)
        finally:
            log.info(f"--- {name} finished")


if __name__ == "__main__":
    main()
