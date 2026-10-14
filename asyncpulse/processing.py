import asyncio
import hashlib
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context
from pathlib import Path


def fingerprint(payload: bytes, rounds: int) -> str:
    for _ in range(rounds):
        payload = hashlib.sha256(payload).digest()
    return payload.hex()


async def cpu_fingerprint(payload: bytes, rounds: int = 100000) -> str:
    loop = asyncio.get_running_loop()
    pool = ProcessPoolExecutor(max_workers=1, mp_context=get_context("spawn"))
    try:
        return await loop.run_in_executor(pool, fingerprint, payload, rounds)
    finally:
        await asyncio.to_thread(pool.shutdown, wait=True, cancel_futures=True)


async def export_json(path: Path, payload: str):
    await asyncio.to_thread(path.write_text, payload, encoding="utf-8")
