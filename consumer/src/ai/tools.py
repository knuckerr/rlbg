import csv
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from pydantic_ai.toolsets import FunctionToolset
from exa_py import Exa

from src.logger import Logger

# Environment variables
EXA_API_KEY = os.getenv("EXA_API_KEY")
exa = Exa(api_key=EXA_API_KEY)


def write_csv(
    filename: str,
    data: List[Dict[str, Any]],
    headers: Optional[List[str]] = None,
) -> str:
    """Write a list of dictionaries to a CSV file."""
    output_dir: Path = Path("./output")
    output_dir.mkdir(parents=True, exist_ok=True)  # ensure folder exists
    try:
        Logger.log("INFO", f"execute write_csv with {data}")
        filename = filename if filename.endswith(".csv") else f"{filename}.csv"
        filepath = output_dir / filename

        if not data:
            return "Error: No data provided to write."

        headers = headers or list(data[0].keys())

        with filepath.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)

        Logger.log(
            "INFO", f"Successfully wrote {len(data)} rows to {filepath.resolve()}"
        )
        return f"CSV successfully written to {filepath.resolve()}"
    except Exception as e:
        Logger.log("ERROR", f"error execute write with csv {e}")
        return f"Error: Failed to write to CSV {e}"


def search_web_with_content(query: str, num_results: int = 10) -> List[Dict[str, Any]]:
    try:
        Logger.log("INFO", f"execute search_web with {query}")
        results = exa.search_and_contents(
            query,
            num_results=num_results,
            use_autoprompt=True,
            text=True,
        )

        normalized = []
        for r in results.results:  # <--- note: attribute access
            normalized.append(
                {
                    "date": getattr(r, "published_date", ""),  # optional, if available
                    "title": getattr(r, "title", ""),
                    "link": getattr(r, "url", ""),
                    "content": getattr(r, "text", "") or getattr(r, "snippet", ""),
                }
            )

        return normalized

    except Exception as e:
        Logger.log("ERROR", f"Error executing search with exa {e}")
        return []


def write_json(filename: str, data: Any) -> str:
    output_dir: Path = Path("./output")
    output_dir.mkdir(parents=True, exist_ok=True)  # ensure folder exists
    Logger.log("INFO", f"execute write_json with {data}")
    try:
        filepath = output_dir / filename
        if not data:
            Logger.log("INFO", f"No data to call write_json")
            return "No data to write in the file"

        with filepath.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            Logger.log("INFO", f"Successfully wrote JSON to {filepath.resolve()}")
            return f"Json successfully written to {filepath.resolve()}"

    except Exception as e:
        Logger.log("ERROR", f"Error writing JSON: {e}")
        return f"Error: Failed to write JSON {e}"


def write_text(filename: str, data: str) -> str:
    output_dir: Path = Path("./output")
    output_dir.mkdir(parents=True, exist_ok=True)  # ensure folder exists
    Logger.log("INFO", f"execute write_text with {data}")
    try:
        filepath = output_dir / filename
        if not data:
            Logger.log("INFO", f"No data to call write_text")
            return "No data to write in the file"

        with filepath.open("w", encoding="utf-8") as f:
            f.write(data)
            Logger.log("INFO", f"Successfully wrote text to {filepath.resolve()}")
            return f"Text successfully written to {filepath.resolve()}"

    except Exception as e:
        Logger.log("ERROR", f"Error writing text: {e}")
        return f"Error: Failed to write text ({e})"


search_and_write = FunctionToolset(
    tools=[search_web_with_content, write_csv, write_json, write_text]
)
