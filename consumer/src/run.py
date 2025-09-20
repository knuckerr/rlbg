import threading
import queue
import uuid

from .client import Client

HOST = "127.0.0.1"
PORT = 4000

# Sharded Queue
job_queue = queue.Queue()


# Reead messages from the client
def reader_worker(stop_reader: threading.Event):
    client = Client(HOST, PORT)
    while not stop_reader.is_set():
        job_id = uuid.uuid4().hex
        job = client.ack_job(job_id)
        if not job:
            continue

    client.close()


def main():
    stop_event = threading.Event()
    # Reader
    reader = threading.Thread(target=reader_worker, args=(stop_event,), name="Reader")
    reader.start()

    # Wait for reader to finish (after CONTROL)
    reader.join()

    print("All jobs processed. Consumer shutting down.")


if __name__ == "__main__":
    main()
