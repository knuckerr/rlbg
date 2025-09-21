# AI Broker and Consumer

## Overview
This project is split into **two main parts**:

- **Broker** → written in **Rust**, built only with the **standard library** (no extra dependencies).  
- **Consumer** → a **Python service** that connects to the broker, fetches jobs, and runs them through an AI.  

The goal is to experiment with an **async AI job processing pipeline**, where messages flow through a custom broker, get picked up by a consumer, and then processed by an AI system.  

⚠️ This is **for learning purposes only** — the whole idea is to discover new places where AI can fit.  

---

## Broker (Rust)
- Built using **only the STD library**.  
- Implements its **own custom protocol** for encoding and decoding messages.  
- Includes its **own thread pool** for handling client connections and requests.  
- Acts as the central **message queue** where jobs are pushed and stored until fetched.  

---

## Consumer (Python)
- Connects to the **Rust broker** using the same protocol to **encode/decode messages**.  
- Fetches jobs from the broker’s queue.  
- Each job is passed to an **AI agent** (using a system prompt + user context).  
- The AI processes the job and outputs the results into a **file**.  
- Designed to scale with multiple worker threads to handle jobs concurrently.  

---

## Flow
1. **Producer/Client** sends a job → Broker.  
2. **Broker** encodes and stores the job in its queue.  
3. **Consumer** fetches a job, decodes it.  
4. **AI Agent** processes the job based on system prompt + context.  
5. **Consumer** writes the result to a file.  

---

## Why This?
- Build everything from scratch → no dependencies in the broker.  
- Understand how **protocols, queues, and job distribution** work at a low level.  
- See how AI can be plugged into a **distributed async system**.  
- A playground to try new ideas (search, summarization, file generation, etc.).  

---

## Notes
- This is not production-ready — it’s a **learning project**.  
- Expect things to break, change, and evolve.  
- The fun part is experimenting with **how AI can fit in real job pipelines**.  
