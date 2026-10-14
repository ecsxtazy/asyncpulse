import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt

with Path(sys.argv[1]).open() as handle:
    rows = [row for row in csv.DictReader(handle) if row["Name"] == "Aggregated"]

fig, axes = plt.subplots(3, 1, figsize=(10, 9), sharex=True)
times = [float(row["Timestamp"]) - float(rows[0]["Timestamp"]) for row in rows]
for key in ("50%", "95%"):
    axes[0].plot(
        times, [float(row[key]) if row[key] != "N/A" else float("nan") for row in rows], label=key
    )
axes[0].set_ylabel("Latency, ms")
axes[0].legend()
axes[1].plot(times, [float(row["Requests/s"]) for row in rows])
axes[1].set_ylabel("Requests/s")
axes[2].plot(times, [float(row["Failures/s"]) for row in rows])
axes[2].set_ylabel("Failures/s")
axes[2].set_xlabel("Time, seconds")
fig.tight_layout()
Path("artifacts").mkdir(exist_ok=True)
fig.savefig("artifacts/load.png", dpi=160)
