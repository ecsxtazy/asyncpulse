import asyncio
import json
import statistics
from pathlib import Path

import httpx

from asyncpulse.processing import export_json


async def main():
    measurements = {}
    async with httpx.AsyncClient(timeout=30) as client:
        for mode in ("sequential", "concurrent"):
            values = []
            for index in range(5):
                response = await client.post(
                    "http://127.0.0.1:8000/search",
                    json={
                        "query": f"benchmark-{mode}-{index}",
                        "mode": mode,
                        "use_cache": False,
                    },
                )
                response.raise_for_status()
                result = response.json()
                if result["status"] != "completed" or any(r["error"] for r in result["results"]):
                    raise RuntimeError(result)
                values.append(result["elapsed_ms"])
            measurements[mode] = {"samples_ms": values, "median_ms": statistics.median(values)}
    Path("artifacts").mkdir(exist_ok=True)
    await export_json(Path("artifacts/benchmark.json"), json.dumps(measurements, indent=2))
    print(json.dumps(measurements, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
