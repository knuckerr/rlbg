import os
from typing import Any, Dict
from pathlib import Path

from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider
from pydantic_ai import Agent


from src.logger import Logger
from src.ai.tools import search_and_write

# Environment variables
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:8b")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434/v1")
EXA_API_KEY = os.getenv("EXA_API_KEY")
AI_OUTPUT_FOLDER = os.getenv("AI_OUTPUT_FOLDER", "./ai_output")

# Model setup
ollama_model = OpenAIChatModel(
    model_name=OLLAMA_MODEL,
    provider=OllamaProvider(base_url=OLLAMA_HOST),
)

agent = Agent(
    name="GenericAgent",
    model=ollama_model,
)


def save_output(job: Dict[str, Any], response: str):
    output_dir: Path = Path(AI_OUTPUT_FOLDER)
    output_dir.mkdir(parents=True, exist_ok=True)  # ensure folder exists
    filepath = output_dir / f"job_{job['id']}_respose.txt"
    with filepath.open("w", newline="", encoding="utf-8") as f:
        f.write(response)


def process_job(job: Dict[str, Any]):
    """Run an agent job with the given system prompt and query."""
    agent.system_prompt = job["system_prompt"]
    response = agent.run_sync(job["params"]["query"], toolsets=[search_and_write])
    Logger.log("INFO", f"Finish with the response {response.output}")
    save_output(job, response.output)
    return response
