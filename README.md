
For learning purpose only - not production ready

# RLBG — Async AI Job Pipeline
RLBG is a **high-performance AI job processing system** is built with:

- **Rust broker**: handles job queue, threading, and custom binary protocol. Zero dependencies, uses `std` only.  
- **Python async consumer**: pulls jobs from broker, runs AI tasks (Ollama + Exa), and writes outputs asynchronously.  
- Fully **async end-to-end** pipeline (network, AI inference).  

---

## Architecture Overview

```text
[Rust Broker]  <--TCP-->  [Python Async Consumer]
       |                        |
       |                        v
       |                 Async Queue (job_queue)
       |                        |
       |                +----------------+
       |                | Async AI Worker|
       |                +----------------+
       |                        |
       |                Writes to CSV/JSON/TXT (async)
       |                        |
       |                 AsyncLogger (colored, exc_info)
````

* **Broker**: implemented in Rust, supports JobPush, JobAck, JobResult, JobStatus, AiQuery, AiResponse, and Control messages.
* **Consumer**: Python async, reads jobs with `AsyncClient`, processes with AI, writes files asynchronously.
* **Logging**: fully async logger, supports exception tracebacks (`exc_info=True`) and ANSI colors.

---

## Rust Broker Protocol

* **Header**: 12 bytes

| Field      | Size | Description                              |
| ---------- | ---- | ---------------------------------------- |
| Magic      | 4    | "RBQ1"                                   |
| Version    | 1    | Protocol version (1)                     |
| Type       | 1    | Message type (`JobPush`, `JobAck`, etc.) |
| Flags      | 2    | Optional flags                           |
| PayloadLen | 4    | Length of TLV payload                    |

* **Payload**: TLV sequence

```text
Tag | Len | Value
------------------
01  | 0005| job42
03  | 0004| [payload length]
07  | 0008| [timestamp]
```

* Supported **MessageTypes**:

```rust
JobPush = 0x01
JobAck = 0x02
JobResult = 0x03
JobStatus = 0x04
AiQuery = 0x10
AiResponse = 0x11
Control = 0x20
```

* TLVs are encoded as:

```text
Tag: u8
Length: u16 (big endian)
Value: variable
```

---

## Python Async Consumer

* **AsyncClient**: replaces synchronous socket client, fully non-blocking.
* **AsyncQueue**: Python `asyncio.Queue` for job processing.
* **AI Workers**: run Ollama + Exa asynchronously.
* **File writes**: using (CSV, JSON, TXT).
* **Logger**: async, supports colors and `exc_info`.

---

### Example: Async Job Reader

```python
async def reader_worker(stop_event: asyncio.Event):
    client = AsyncClient(HOST, PORT)
    await client.connect()
    try:
        while not stop_event.is_set():
            job_id = uuid.uuid4().hex
            job = await client.ack_job(job_id)
            if job:
                await job_queue.put(job)
                await logger.log("INFO", f"Received job {job}")
            else:
                await asyncio.sleep(0.1)
    finally:
        await client.close()
        await logger.log("INFO", "Reader worker stopped")
```

---

### Example: Async Job Submission

```python
client = AsyncClient("127.0.0.1", 4000)
await client.connect()
job_id = "forced_events_job"
prompt = {
    "system_prompt": "You are the best websearch and csv creator",
    "params": {"query": "Find insights into AI's impact and save to hot_news.csv"}
}
await client.push_job(job_id, json.dumps(prompt).encode())
await client.close()
```

---

### Async Logger Usage

```python
try:
    1 / 0
except Exception:
    await logger.log("ERROR", "Division failed", exc_info=True)
```

* Supports **async logging**, ANSI colors, and traceback capture.

---

## Docker & Make Commands

```makefile
.DEFAULT_GOAL := help
.PHONY: help up down logs build broker-sh consumer-sh python-lint rust-lint

help: ## Show available commands
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

up: ## Start docker-compose
	docker-compose up -d

logs: ## Tail logs from docker-compose
	docker-compose logs -f

down: ## Stop docker-compose
	docker-compose down

build: ## Build docker-compose
	docker-compose build

broker-sh: ## Open a bash in broker container
	docker exec -it rust_broker bash

consumer-sh: ## Open a bash in consumer container
	docker exec -it python_consumer bash

python-lint: ## Run Black linter
	black consumer

rust-lint: ## Run Rust fmt
	cargo fmt --manifest-path broker/Cargo.toml
```

---

## Environment Variables

| Variable           | Default                  | Description                |
| ------------------ | ------------------------ | -------------------------- |
| `HOST`             | `broker`                 | Broker hostname            |
| `PORT`             | `4000`                   | Broker port                |
| `MAX_AI_WORKERS`   | `4`                      | Max concurrent AI workers  |
| `MAX_QUEUE_SIZE`   | `500`                    | Async job queue size       |
| `OLLAMA_MODEL`     | `qwen3:8b`               | Ollama model for inference |
| `OLLAMA_HOST`      | `http://127.0.0.1:11434` | Ollama API host            |
| `EXA_API_KEY`      | None                     | Exa API key for web search |
| `AI_OUTPUT_FOLDER` | `./ai_output`            | Folder to save AI outputs  |

---

## Features

* Fully async AI pipeline: **network, AI inference, logging**.
* AsyncClient supports **JobAck, JobPush** over Rust broker protocol.
* Async logging with **colors** and **exception tracebacks**.
* Scales to **hundreds of concurrent jobs** without thread blocking.
* Dockerized environment for easy deployment and testing.

```
