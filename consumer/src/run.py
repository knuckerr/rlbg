import threading
import queue
from queue import Empty
import uuid
import os
import signal
import time

from src.client import Client
from src.logger import Logger

# Configuration
HOST = os.getenv("HOST", "broker")
PORT = int(os.getenv("PORT", 4000))
MAX_AI_WORKERS = int(os.getenv("MAX_AI_WORKERS", 4))
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", 500))  # bounded queue

# Shared queue
job_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)


def reader_worker(stop_event: threading.Event):
    Logger.log("INFO", "Reader worker starting", host=HOST, port=PORT)
    client = Client(HOST, PORT)
    while not stop_event.is_set():
        job_id = uuid.uuid4().hex
        job = client.ack_job(job_id)
        if job is None:
            time.sleep(0.1)  # avoid busy wait
            continue
        Logger.log("DEBUG", "Fetched job", job_id=job.get(1))

        # Backpressure: blocks if queue is full
        try:
            job_queue.put(job, timeout=1)
            Logger.log("INFO", f"Recieved a job {job}")
        except queue.Full:
            Logger.log("WARNING", "Job queue full, retrying", job_id=job.get(1))
            continue

    client.close()
    Logger.log("INFO", "Reader worker stopping")


def ai_worker(stop_event: threading.Event, worker_id: int):
    Logger.log("INFO", f"AI worker {worker_id} starting")
    while not stop_event.is_set():
        try:
            job = job_queue.get(timeout=1)
        except Empty:
            continue

        Logger.log("INFO", f"AI worker {worker_id} processing job", job_id=job.get(1))
        # process_job(job)
        job_queue.task_done()

    Logger.log("INFO", f"AI worker {worker_id} stopping")


def main():
    stop_event = threading.Event()

    # Signal handling for graceful shutdown
    def shutdown_handler(sig, frame):
        Logger.log("WARNING", f"Received signal {sig}, shutting down...")
        stop_event.set()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Start reader
    reader = threading.Thread(target=reader_worker, args=(stop_event,), name="Reader")
    reader.start()

    # Start AI workers
    ai_threads = []
    for i in range(MAX_AI_WORKERS):
        t = threading.Thread(target=ai_worker, args=(stop_event, i), name=f"AI_{i}")
        t.start()
        ai_threads.append(t)

    # Wait for shutdown
    reader.join()
    for t in ai_threads:
        t.join()

    Logger.log("INFO", "Consumer shutdown complete.")


if __name__ == "__main__":
    main()
