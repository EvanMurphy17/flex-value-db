# derdata/utils/io.py
import gzip
import json
from pathlib import Path
from typing import Any


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_json_gz(obj: Any, path: Path) -> None:
    ensure_dir(path.parent)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)


def read_json(path: Path) -> Any | None:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)
