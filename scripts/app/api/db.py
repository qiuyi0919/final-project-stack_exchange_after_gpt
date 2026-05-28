from __future__ import annotations

import os
import sqlite3
from pathlib import Path


SCRIPTS_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = SCRIPTS_ROOT / "data" / "serving.db"


def database_path() -> Path:
    return Path(os.environ.get("CQI_DB_PATH", DEFAULT_DB_PATH)).expanduser().resolve()


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(database_path())
    conn.row_factory = sqlite3.Row
    return conn

