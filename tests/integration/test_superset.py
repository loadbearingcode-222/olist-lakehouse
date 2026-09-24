"""Superset from nothing: the committed bundle alone must bring back all four dashboards.

The test drops Superset's metadata database (its whole state: users, the Trino connection,
datasets, charts, dashboards), recreates it with platform-db-init, runs superset-init (which
imports `superset/dashboards/`), starts Superset, and then asks the API for every dashboard's
charts and every chart's data. A chart that renders nothing fails the test, not just one that
errors.

Evidence: automation/runs/m5-s1/superset.json. Takes about 2 minutes.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from superset_dashboards import CHARTS, Api

pytestmark = pytest.mark.integration

REPO = Path(__file__).resolve().parents[2]
EVIDENCE = REPO / "automation" / "runs" / "m5-s1" / "superset.json"


def compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def checked(*args: str) -> str:
    result = compose(*args)
    assert result.returncode == 0, f"docker compose {' '.join(args)}\n{result.stderr[-2000:]}"
    return result.stdout + result.stderr


@pytest.fixture(scope="module")
def rebuilt() -> str:
    try:
        socket.create_connection(("127.0.0.1", 28088), timeout=2).close()
    except OSError:
        if os.environ.get("OLIST_STACK_REQUIRED") == "1":
            pytest.fail("Superset is not reachable on 127.0.0.1:28088")
        pytest.skip("Superset is not up")
    checked("stop", "superset")
    checked(
        "exec", "-T", "platform-db", "psql", "-U", "platform", "-d", "lakekeeper",
        "-Xqc", "DROP DATABASE superset WITH (FORCE)",
    )  # fmt: skip
    checked("run", "--rm", "--no-deps", "platform-db-init")
    init_log = checked("run", "--rm", "--no-deps", "superset-init")
    checked("up", "-d", "--wait", "--no-deps", "superset")
    return init_log


def test_a_fresh_superset_imports_four_dashboards_that_show_data(rebuilt: str) -> None:
    api = Api()
    dashboards = api.call("GET", "/api/v1/dashboard/?q=(page_size:100)")["result"]
    found: dict[str, dict[str, int]] = {}
    for dashboard in dashboards:
        charts = api.call("GET", f"/api/v1/dashboard/{dashboard['id']}/charts")["result"]
        rows = {}
        for chart in charts:
            data = api.call("GET", f"/api/v1/chart/{chart['id']}/data/")["result"][0]
            rows[chart["slice_name"]] = int(data["rowcount"])
        found[dashboard["dashboard_title"]] = rows

    evidence = {
        "checked_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "fresh_metadata_db": True,
        "imported": "dashboards imported from the committed bundle" in rebuilt,
        "dashboards": found,
    }
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE.write_bytes((json.dumps(evidence, indent=2) + "\n").encode("utf-8"))
    print("\n" + json.dumps(evidence, indent=1))

    assert evidence["imported"]
    assert set(found) == set(CHARTS)
    for title, charts in CHARTS.items():
        assert set(found[title]) == {c["name"] for c in charts}, title
        assert all(n > 0 for n in found[title].values()), (title, found[title])
