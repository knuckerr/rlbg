import asyncio
from queue import Empty
import uuid
import os
import signal
import time

from src.client import Client
from src.logger import logger
from src.ai import process_job

# Configuration
HOST = os.getenv("HOST", "broker")
PORT = int(os.getenv("PORT", 4000))
MAX_AI_WORKERS = int(os.getenv("MAX_AI_WORKERS", 4))
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", 500))  # bounded queue

# Shared queue
job_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)


async def reader_worker(stop_event: asyncio.Event):
    await logger.log("INFO", "Reader worker starting", host=HOST, port=PORT)
    client = Client(HOST, PORT)
    await client.connect()
    try:
        while not stop_event.is_set():
            job_id = uuid.uuid4().hex
            job = await client.ack_job(job_id)
            if not job:
                await asyncio.sleep(0.1)
                continue

            job["id"] = job_id
            await job_queue.put(job)
            await logger.log("INFO", f"Received job {job}")
    finally:
        await client.close()
        await logger.log("INFO", "Reader worker stopping")


async def ai_worker(stop_event: asyncio.Event, worker_id: int):
    await logger.log("INFO", f"AI worker {worker_id} starting")
    try:
        while not stop_event.is_set():
            try:
                job = await asyncio.wait_for(job_queue.get(), timeout=1)
            except asyncio.TimeoutError:
                continue

            try:
                await logger.log(
                    "INFO",
                    f"AI worker {worker_id} processing job",
                    job_id=job.get("id"),
                )
                await process_job(job)
            finally:
                job_queue.task_done()
    finally:
        await logger.log("INFO", f"AI worker {worker_id} stopping")


async def main():
    logger.start()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    # Signal handling for graceful shutdown
    def shutdown_handler():
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_handler)

    # Start reader
    reader_task = asyncio.create_task(reader_worker(stop_event))

    # Start AI workers
    ai_tasks = [
        asyncio.create_task(ai_worker(stop_event, i)) for i in range(MAX_AI_WORKERS)
    ]

    await stop_event.wait()
    await logger.log("INFO", "Waiting for queue to drain...")
    await job_queue.join()

    reader_task.cancel()
    for t in ai_tasks:
        t.cancel()

    await logger.log("INFO", "Consumer shutdown complete.")
    await logger.stop()


if __name__ == "__main__":
    asyncio.run(main())
