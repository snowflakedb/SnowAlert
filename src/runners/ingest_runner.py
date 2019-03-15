from runners.helpers import log
import os
import subprocess


#  The pipeline runner needs to list all the files in the pipeline folder and then invoke them one by one.

def main():
    for name in os.listdir('../ingestion'):
        log.info(f"invoking {name}")
        try:
            res = subprocess.call(f"python ../ingestion/{name}", shell=True)
            log.info("subprocess returns: ", res)
            log.info(f"{name} invoked")
        except Exception as e:
            log.error(f"failed to run {name}", e)


if __name__ == "__main__":
    main()
