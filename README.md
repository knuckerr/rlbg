

# AI Broker and Consumer

## Overview

This project is split into **two main parts**:

* **Broker** → written in **Rust**, built only with the **standard library** (no extra dependencies).
* **Consumer** → a **Python service** that connects to the broker, fetches jobs, and runs them through an **AI agent**.

The goal is to experiment with an **async AI job processing pipeline**, where messages flow through a custom broker, get picked up by a consumer, and then processed by an AI system.

⚠️ This is **for learning purposes only** — the whole idea is to discover new places where AI can fit.

---

## Broker (Rust)

* Built using **only the STD library**.
* Implements its **own custom protocol** for encoding and decoding messages.
* Includes its **own thread pool** for handling client connections and requests.
* Acts as the central **message queue** where jobs are pushed and stored until fetched.

---

## Consumer (Python)

* Connects to the **Rust broker** using the same protocol to **encode/decode messages**.
* Fetches jobs from the broker’s queue.
* Each job is passed to an **AI agent** powered by:

  * **Ollama** → runs LLMs locally.
  * **Strands agents** → used as lightweight orchestrators for chaining prompts, context, and job logic.
* The AI processes the job and outputs the results into a **file**.
* Designed to scale with multiple worker threads to handle jobs concurrently.

---

## Flow

1. **Producer/Client** sends a job → Broker.
2. **Broker** encodes and stores the job in its queue.
3. **Consumer** fetches a job, decodes it.
4. **AI Agent** (Ollama + Strands) processes the job based on system prompt + context.
5. **Consumer** writes the result to a file.

---

## Why This?

* Build everything from scratch → no dependencies in the broker.
* Understand how **protocols, queues, and job distribution** work at a low level.
* See how AI can be plugged into a **distributed async system**.
* A playground to try new ideas (search, summarization, file generation, etc.).

---

## Notes

* This is not production-ready — it’s a **learning project**.
* Expect things to break, change, and evolve.
* The fun part is experimenting with **how AI can fit in real job pipelines**.

---

## Running with Docker Compose

The project ships with a `docker-compose.yml` setup:

```yaml
version: "3.9"

services:
  broker:
    build: ./broker
    container_name: rust_broker
    ports:
      - "4000:4000"

  consumer:
    build: ./consumer
    container_name: python_consumer
    depends_on:
      - broker
    environment:
      HOST: broker
      PORT: 4000
      OLLAMA_HOST: http://host.docker.internal:11434
    volumes:
      - ./consumer/src:/usr/src/consumer/src
```

### Steps:

1. Start everything:

   ```bash
   docker-compose up --build
   ```
2. Ensure **Ollama** is running locally (default: `http://localhost:11434`).
3. Push jobs with a Python client.

---

## Example Client Usage

Here’s how to push a job into the broker from Python:

```python
import json
from client import Client  # assuming you have a simple broker client

client = Client("127.0.0.1", 4000)

job_data = {
    "system_prompt": "You are a master mind in C++",
    "params": {
        "query": "what are some safe approaches to avoid memory leaks?",
        "output_file": "best_practises.txt"
    }
}

# Push job into the broker
result = client.push_job("job1", json.dumps(job_data).encode())
print("Job pushed:", result)
```

The consumer will:

1. Pick up the job.
2. Pass it through **Ollama + Strands agent**.
3. Write the output into `best_practises.txt`.
