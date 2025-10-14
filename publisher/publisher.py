import asyncio
import json
import argparse
from consumer.src.client import Client

async def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Send an AI job to the broker")
    parser.add_argument("-q", "--query", required=True, help="The query to send")
    parser.add_argument(
        "-s",
        "--system-prompt",
        default="You are the best websearch and csv creator",
        help="The system prompt for the agent",
    )
    parser.add_argument(
        "-j", "--job-id", default="user_job", help="Optional job ID"
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="Broker host"
    )
    parser.add_argument(
        "--port", type=int, default=4000, help="Broker port"
    )

    args = parser.parse_args()

    # Start logger

    client = Client(args.host, args.port)
    await client.connect()

    prompt = {
        "system_prompt": args.system_prompt,
        "params": {"query": args.query}
    }

    print("INFO", "=== SENDING JOB ===")
    print("INFO", f"Query: {args.query}")
    print("INFO", f"System prompt: {args.system_prompt}")

    # Push job asynchronously
    success = await client.push_job(args.job_id, json.dumps(prompt).encode())
    if success:
        print("INFO", f"Job '{args.job_id}' submitted successfully")
    else:
        print("ERROR", f"Failed to submit job '{args.job_id}'")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
