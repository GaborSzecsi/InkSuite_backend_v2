# services/file_os.py
import os
import json
from typing import Any

def load_json(file_path: str, default: Any = None) -> Any:
    """
    Load JSON from disk. If the file doesn't exist or is invalid JSON,
    return `default` (or [] if default is None).
    """
    # Ensure parent directory exists (handy on first run)
    parent = os.path.dirname(file_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default if default is not None else []
    except json.JSONDecodeError:
        return default if default is not None else []

def save_json(file_path: str, data: Any) -> None:
    """
    Save JSON to disk atomically to avoid partial writes.
    """
    parent = os.path.dirname(file_path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    tmp_path = file_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, file_path)
