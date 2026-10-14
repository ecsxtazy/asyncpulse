import csv
import sys
import time
from pathlib import Path

import psutil

process = psutil.Process(int(sys.argv[1]))
Path("artifacts").mkdir(exist_ok=True)
with Path("artifacts/memory.csv").open("w") as handle:
    writer = csv.writer(handle)
    writer.writerow(["seconds", "rss_mb"])
    start = time.monotonic()
    try:
        while process.is_running():
            writer.writerow(
                [round(time.monotonic() - start, 2), process.memory_info().rss / 1024**2]
            )
            handle.flush()
            time.sleep(1)
    except (KeyboardInterrupt, psutil.NoSuchProcess):
        pass
