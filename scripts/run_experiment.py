import os
import subprocess
import sys
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path

import httpx


@contextmanager
def open_log(path):
    with path.open("w") as handle:
        yield handle


processes = []
Path("artifacts").mkdir(exist_ok=True)
files = ExitStack()
try:
    for module, port in (("asyncpulse.mock", 8001), ("asyncpulse.main", 8000)):
        log = files.enter_context(open_log(Path(f"artifacts/{port}.log")))
        environment = dict(os.environ)
        environment.pop("REDIS_URL", None)
        environment.pop("DATABASE_URL", None)
        processes.append(
            subprocess.Popen(
                [sys.executable, "-m", "uvicorn", f"{module}:app", "--port", str(port)],
                stdout=log,
                stderr=log,
                env=environment,
            )
        )
    with httpx.Client(timeout=1) as client:
        for port, endpoint in ((8001, "/openapi.json"), (8000, "/health")):
            for _ in range(100):
                if any(process.poll() is not None for process in processes):
                    raise RuntimeError("Server exited; inspect artifacts/*.log")
                try:
                    client.get(f"http://127.0.0.1:{port}{endpoint}").raise_for_status()
                    break
                except httpx.HTTPError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("Server did not become ready")
    subprocess.run([sys.executable, "scripts/benchmark.py"], check=True)
    memory = subprocess.Popen([sys.executable, "scripts/memory.py", str(processes[1].pid)])
    processes.append(memory)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "locust",
            "-f",
            "load/locustfile.py",
            "--host",
            "http://127.0.0.1:8000",
            "--headless",
            "-u",
            "20",
            "-r",
            "5",
            "-t",
            "30s",
            "--csv",
            "artifacts/load",
            "--csv-full-history",
            "--stop-timeout",
            "20",
        ],
        check=True,
    )
    memory.terminate()
    memory.wait(timeout=5)
    for script, args in (
        ("plot_load.py", ["artifacts/load_stats_history.csv"]),
        ("plot_memory.py", []),
    ):
        subprocess.run(
            [sys.executable, f"scripts/{script}", *args],
            check=True,
            env={**os.environ, "MPLCONFIGDIR": "/tmp/asyncpulse-matplotlib"},
        )
finally:
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    files.close()
