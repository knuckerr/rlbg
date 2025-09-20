import threading
import queue
import uuid
import os

from .client import Client
from .logger import Logger

HOST = os.getenv("HOST", "broker")
PORT = int(os.getenv("PORT", 4000))


# Sharded Queue
job_queue = queue.Queue()


# Reead messages from the client
def reader_worker(stop_reader: threading.Event):
    Logger.log("INFO", "Reader worker starting", host=HOST, port=PORT)
    client = Client(HOST, PORT)
    while not stop_reader.is_set():
        job_id = uuid.uuid4().hex
        job = client.ack_job(job_id)
        if job is None:
            continue
        Logger.log("INFO", "Fetched job", job_id=job.get(1))
    client.close()
    Logger.log("INFO", "Reader worker stopping")


def main():
    Logger.log("INFO", "Consumer starting")
    stop_event = threading.Event()
    # Reader
    reader = threading.Thread(target=reader_worker, args=(stop_event,), name="Reader")
    reader.start()

    # Wait for reader to finish (after CONTROL)
    reader.join()

    Logger.log("INFO", "All jobs processed. Consumer shutting down.")


if __name__ == "__main__":
    main()
