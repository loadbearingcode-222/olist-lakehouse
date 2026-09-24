"""Paths and names every module agrees on. Override with environment variables, never edits."""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
KAGGLE_BIN = os.environ.get("KAGGLE_BIN", "kaggle")

DATA_DIR = Path(os.environ.get("OLIST_DATA_DIR", REPO_ROOT / "data"))
RAW_DIR = DATA_DIR / "raw" / "olist"
MANIFEST = DATA_DIR / "olist.sha256"
