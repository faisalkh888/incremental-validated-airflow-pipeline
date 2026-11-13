from __future__ import annotations

import os
from pathlib import Path


def data_dir() -> Path:
    return Path(os.getenv("PIPELINE_DATA_DIR", Path(__file__).resolve().parents[1] / "data"))


def source_base_url() -> str:
    return os.getenv("SOURCE_BASE_URL", "https://dummyjson.com").rstrip("/")

