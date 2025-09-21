job_schema = {
    "type": "object",
    "properties": {
        "params": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1},
            },
            "required": ["query"],  # ✅ enforce query
            "additionalProperties": True,  # allow extra params beyond query & limit
        },
        "system_prompt": {"type": "string"},
        "output": {"type": "string"},
    },
    "required": ["params", "system_prompt"],
    "additionalProperties": False,
}
