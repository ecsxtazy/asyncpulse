import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field

from .models import SearchRequest, SearchResult

logger = logging.getLogger(__name__)

TERMINAL = {"completed", "cancelled", "failed"}


@dataclass
class Job:
    request: SearchRequest
    result: SearchResult
    done: asyncio.Event = field(default_factory=asyncio.Event)
    changed: asyncio.Condition = field(default_factory=asyncio.Condition)
    revision: int = 0
    task: asyncio.Task | None = None

    async def notify(self):
        async with self.changed:
            self.revision += 1
            self.changed.notify_all()


class SearchService:
    def __init__(self, settings, scheduler, history):
        self.settings = settings
        self.scheduler = scheduler
        self.history = history
        self.queue = asyncio.Queue(maxsize=settings.queue_size)
        self.jobs: dict[str, Job] = {}
        self.workers = []

    async def start(self):
        self.workers = [asyncio.create_task(self.worker()) for _ in range(self.settings.workers)]

    def submit(self, request: SearchRequest) -> Job:
        sources = request.sources or list(self.settings.sources)
        if any(source not in self.settings.sources for source in sources):
            raise ValueError("Unknown source")
        if len(set(sources)) != len(sources):
            raise ValueError("Duplicate sources")
        if len(self.jobs) >= self.settings.max_jobs:
            expired = next((key for key, job in self.jobs.items() if job.done.is_set()), None)
            if expired:
                del self.jobs[expired]
            else:
                raise asyncio.QueueFull
        job_id = str(uuid.uuid4())
        job = Job(request, SearchResult(id=job_id, query=request.query, status="queued"))
        self.queue.put_nowait(job)
        self.jobs[job_id] = job
        return job

    async def cancel(self, job: Job):
        if job.result.status in TERMINAL:
            return
        job.result.status = "cancelled"
        if job.task:
            job.task.cancel()
            await asyncio.gather(job.task, return_exceptions=True)
        job.done.set()
        await job.notify()

    async def worker(self):
        while True:
            job = await self.queue.get()
            try:
                if job.result.status == "cancelled":
                    continue
                job.task = asyncio.create_task(self.run(job))
                await asyncio.gather(job.task, return_exceptions=True)
            finally:
                self.queue.task_done()

    async def run(self, job: Job):
        started = time.monotonic()
        job.result.status = "running"
        await job.notify()

        async def fetch(source):
            result = await self.scheduler.fetch(source, job.request.query, job.request.use_cache)
            job.result.results.append(result)
            await job.notify()

        try:
            async with asyncio.timeout(self.settings.search_timeout):
                sources = job.request.sources or list(self.settings.sources)
                if job.request.mode == "sequential":
                    for source in sources:
                        await fetch(source)
                else:
                    async with asyncio.TaskGroup() as group:
                        for source in sources:
                            group.create_task(fetch(source))
            job.result.status = "completed"
        except asyncio.CancelledError:
            job.result.status = "cancelled"
            raise
        except TimeoutError:
            job.result.status = "failed"
            job.result.error = "Search deadline exceeded"
        except Exception:
            logger.exception("Search execution failed")
            job.result.status = "failed"
            job.result.error = "Search infrastructure error"
        finally:
            job.result.elapsed_ms = (time.monotonic() - started) * 1000
            try:
                async with asyncio.timeout(2):
                    await self.history.save(job.result)
            except Exception:
                logger.exception("History persistence failed")
                job.result.error = job.result.error or "History persistence failed"
            finally:
                job.done.set()
                await job.notify()

    async def close(self):
        await asyncio.gather(*(self.cancel(job) for job in self.jobs.values()))
        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        await self.scheduler.close()
