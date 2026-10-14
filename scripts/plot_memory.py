import csv
from pathlib import Path

import matplotlib.pyplot as plt

with Path("artifacts/memory.csv").open() as handle:
    rows = list(csv.DictReader(handle))
plt.plot([float(row["seconds"]) for row in rows], [float(row["rss_mb"]) for row in rows])
plt.xlabel("Time, seconds")
plt.ylabel("RSS, MB")
plt.tight_layout()
plt.savefig("artifacts/memory.png", dpi=160)
