import csv
import os
from pathlib import Path
from typing import Any, Dict

from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.ollama import OllamaProvider
from pydantic_ai import Agent
from pydantic import BaseModel, Field


from src.logger import Logger
from src.ai.tools import search_and_write

# Environment variables
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:8b")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434/v1")
EXA_API_KEY = os.getenv("EXA_API_KEY")


# Model setup
ollama_model = OpenAIChatModel(
    model_name=OLLAMA_MODEL,
    provider=OllamaProvider(base_url=OLLAMA_HOST),
)

agent = Agent(
    name="GenericAgent",
    model=ollama_model,
)


def process_job(job: Dict[str, Any]):
    """Run an agent job with the given system prompt and query."""
    agent.system_prompt = job["system_prompt"]
    response = agent.run_sync(job["params"]["query"], toolsets=[search_and_write])
    Logger.log("INFO", response.output)
    return response
