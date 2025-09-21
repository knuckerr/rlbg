import os

from strands import Agent
from strands_tools import file_write, file_read
from strands.models.ollama import OllamaModel

OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:8b")
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://127.0.0.1:11434")

def process_job(job: dict):
    agend = Agent(
        name="GenericAgent",
        model= OllamaModel(model_id=OLLAMA_MODEL, host=OLLAMA_HOST),
        system_prompt=job['system_prompt'],
        tools=[file_write, file_read]
    )
    response = agend(job['params']['query'])
    return response
