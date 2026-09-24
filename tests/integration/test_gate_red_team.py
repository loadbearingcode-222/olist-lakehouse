"""The gate's red team, run as a test: every planted defect is refused under its own rule.

Wraps `olist_lakehouse.quality.redteam`, which judges crafted rows in a throwaway Spark
container. Nothing in the live lake is read or written.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]


def test_every_planted_defect_is_refused_by_name() -> None:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "bronze-stream",
            "/opt/spark/bin/spark-submit",
            "/opt/app/src/olist_lakehouse/quality/redteam.py",
        ],
        cwd=REPO,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    out = result.stdout
    outcome = json.loads(out[out.index("{") : out.rindex("}") + 1])
    failed = [c for c in outcome["cases"] if not c["ok"]]
    assert not failed, failed
    assert outcome["gx_agrees"]
    assert {c["got"] for c in outcome["cases"]} - {None} == {
        "negative_price",
        "null_customer_id",
        "unknown_status",
        "approved_before_purchase",
        "missing_event_type",
        "null_product",
    }
    assert result.returncode == 0
