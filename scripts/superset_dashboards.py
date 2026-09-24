"""Build the four dashboards in a running Superset through its REST API, then export them.

    uv run --group integration python scripts/superset_dashboards.py
    .\\make.ps1 superset-build

The definitions below are the source; `superset/dashboards/` (committed) is their export: the
database connection, datasets, charts and dashboards as YAML, which `superset-init` imports on
every `up`. Rebuilding replaces the four dashboards and their charts, so the script can be
rerun after any change here.

    Revenue            monthly revenue BRL and USD, total USD, orders per month
    Delivery SLA       late-delivery share per month, delivery days by customer state
    Funnel             weekly sessions and conversions, overall conversion, funnel stages
    Stream health      quarantine by rule, gate vs Great Expectations, duplicates and late events
"""

from __future__ import annotations

import http.cookiejar
import io
import json
import shutil
import sys
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
BUNDLE = REPO / "superset" / "dashboards"
BASE = "http://127.0.0.1:28088"
DATABASE = "Olist lake (Trino)"
URI = "trino://superset@trino:8080/lake"


def env(key: str) -> str:
    for line in (REPO / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{key}="):
            return line.split("=", 1)[1]
    raise SystemExit(f".env has no {key}")


class Api:
    def __init__(self) -> None:
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
        )
        login = self.call(
            "POST",
            "/api/v1/security/login",
            {"username": "admin", "password": env("SUPERSET_ADMIN_PASSWORD"), "provider": "db"},
            auth=False,
        )
        self.token = login["access_token"]
        self.csrf = self.call("GET", "/api/v1/security/csrf_token/")["result"]

    def call(
        self, method: str, path: str, body: Any = None, *, auth: bool = True, raw: bool = False
    ) -> Any:
        headers = {"Referer": BASE}
        if body is not None:
            headers["Content-Type"] = "application/json"
        if auth:
            headers["Authorization"] = f"Bearer {self.token}"
            if method != "GET":
                headers["X-CSRFToken"] = self.csrf
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
        try:
            with self.opener.open(request, timeout=60) as response:
                payload = response.read()
        except urllib.error.HTTPError as exc:
            raise SystemExit(f"{method} {path} -> {exc.code}: {exc.read()[:500]!r}") from exc
        return payload if raw else (json.loads(payload) if payload else {})

    def find(self, resource: str, column: str, value: str) -> list[dict[str, Any]]:
        rison = f"(filters:!((col:{column},opr:eq,value:'{value}')),page_size:100)"
        return list(
            self.call("GET", f"/api/v1/{resource}/?q={urllib.parse.quote(rison)}")["result"]
        )


# ------------------------------------------------------------------------------ definitions


def metric(sql: str, label: str) -> dict[str, Any]:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


def time_axis(column: str, grain: str) -> dict[str, Any]:
    return {
        "timeGrain": grain,
        "columnType": "BASE_AXIS",
        "sqlExpression": column,
        "label": column,
        "expressionType": "SQL",
    }


CHARTS: dict[str, list[dict[str, Any]]] = {
    "Revenue": [
        {
            "name": "Revenue per month (BRL and USD)",
            "dataset": "mart_daily_revenue",
            "viz": "echarts_timeseries_line",
            "x": ("purchase_date", "P1M"),
            "metrics": [
                metric("SUM(revenue_brl)", "Revenue BRL"),
                metric("SUM(revenue_usd)", "Revenue USD"),
            ],
            "width": 8,
        },
        {
            "name": "Total revenue (USD)",
            "dataset": "mart_daily_revenue",
            "viz": "big_number_total",
            "metrics": [metric("SUM(revenue_usd)", "Revenue USD")],
            "width": 4,
        },
        {
            "name": "Orders per month",
            "dataset": "mart_daily_revenue",
            "viz": "echarts_timeseries_bar",
            "x": ("purchase_date", "P1M"),
            "metrics": [
                metric("SUM(orders)", "Orders"),
                metric("SUM(flagged_orders)", "Flagged orders"),
            ],
            "width": 12,
        },
    ],
    "Delivery SLA": [
        {
            "name": "Late deliveries per month",
            "dataset": "mart_delivery_sla",
            "viz": "echarts_timeseries_line",
            "x": ("purchase_month", "P1M"),
            "metrics": [
                metric("SUM(late_share * delivered_orders) / SUM(delivered_orders)", "Late share")
            ],
            "width": 6,
        },
        {
            "name": "Average delivery days by customer state",
            "dataset": "mart_delivery_sla",
            "viz": "echarts_timeseries_bar",
            "x": ("customer_state", None),
            "metrics": [
                metric(
                    "SUM(avg_delivery_days * delivered_orders) / SUM(delivered_orders)",
                    "Delivery days",
                )
            ],
            "width": 6,
        },
    ],
    "Funnel": [
        {
            "name": "Sessions and conversions per week",
            "dataset": "mart_funnel_weekly",
            "viz": "echarts_timeseries_line",
            "x": ("week", "P1W"),
            "metrics": [
                metric("SUM(sessions)", "Sessions"),
                metric("SUM(converted_sessions)", "Converted"),
            ],
            "width": 8,
        },
        {
            "name": "Overall conversion rate",
            "dataset": "mart_funnel_weekly",
            "viz": "big_number_total",
            "metrics": [
                metric("CAST(SUM(converted_sessions) AS double) / SUM(sessions)", "Conversion")
            ],
            "width": 4,
        },
        {
            "name": "Funnel stages (all weeks)",
            "dataset": "mart_funnel_weekly",
            "viz": "table",
            "metrics": [
                metric("SUM(sessions)", "Sessions"),
                metric("SUM(sessions_with_cart)", "Cart"),
                metric("SUM(sessions_with_checkout)", "Checkout"),
                metric("SUM(converted_sessions)", "Purchase"),
            ],
            "width": 12,
        },
    ],
    "Stream health": [
        {
            "name": "Quarantined rows by rule",
            "dataset": "mart_stream_health",
            "viz": "echarts_timeseries_bar",
            "x": ("rule", None),
            "where": "kind = 'gate'",
            "metrics": [metric("SUM(quarantined)", "Quarantined")],
            "width": 6,
        },
        {
            "name": "Gate vs Great Expectations",
            "dataset": "mart_stream_health",
            "viz": "table",
            "groupby": ["source", "rule"],
            "where": "kind = 'gate'",
            "metrics": [
                metric("SUM(gate_violations)", "Gate"),
                metric("SUM(gx_unexpected)", "GX"),
                metric("MIN(CASE WHEN gate_and_gx_agree THEN 1 ELSE 0 END)", "Agree (1 = yes)"),
            ],
            "width": 6,
        },
        {
            "name": "Duplicates and late events (measured, handled in silver)",
            "dataset": "mart_stream_health",
            "viz": "table",
            "groupby": ["rule"],
            "where": "kind = 'measure'",
            "metrics": [metric("SUM(gate_violations)", "Events")],
            "width": 12,
        },
    ],
}

NOTES = {
    "Revenue": "Trusted orders only, excluding canceled/unavailable. Flagged orders (silver "
    "`_dq_issues`) are counted separately. USD uses the ECB rate of the purchase day.",
    "Delivery SLA": "Delivered, trusted orders. Late = delivered after the estimated date.",
    "Funnel": "Stages are cumulative: a purchase implies checkout and cart, because earlier "
    "events can be missing (orders without items; quarantined drift events).",
    "Stream health": "The bronze quality gate (ADR-0004): rows refused per rule, with Great "
    "Expectations' independent count beside the gate's.",
}


def form_data(chart: dict[str, Any], dataset_id: int) -> dict[str, Any]:
    fd: dict[str, Any] = {
        "datasource": f"{dataset_id}__table",
        "viz_type": chart["viz"],
        "metrics": chart["metrics"],
        "adhoc_filters": [],
        "row_limit": 10000,
        "time_range": "No filter",
    }
    if chart.get("where"):
        fd["adhoc_filters"] = [
            {"expressionType": "SQL", "sqlExpression": chart["where"], "clause": "WHERE"}
        ]
    if "x" in chart:
        column, grain = chart["x"]
        fd["x_axis"] = column
        if grain:
            fd["time_grain_sqla"] = grain
        fd["groupby"] = []
    if chart["viz"] == "big_number_total":
        fd["metric"] = chart["metrics"][0]
        del fd["metrics"]
    if chart["viz"] == "table":
        fd["query_mode"] = "aggregate"
        fd["groupby"] = chart.get("groupby", [])
    return fd


def query_context(chart: dict[str, Any], dataset_id: int, fd: dict[str, Any]) -> dict[str, Any]:
    columns: list[Any] = list(chart.get("groupby", []))
    if "x" in chart:
        column, grain = chart["x"]
        columns = [time_axis(column, grain) if grain else column]
    filters = fd["adhoc_filters"]
    query = {
        "columns": columns,
        "metrics": chart["metrics"],
        "orderby": [],
        "row_limit": 10000,
        "filters": [],
        "extras": {"where": " AND ".join(f["sqlExpression"] for f in filters), "having": ""},
        "time_range": "No filter",
    }
    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [query],
        "form_data": fd,
        "result_format": "json",
        "result_type": "full",
    }


def position(title: str, charts: list[tuple[int, dict[str, Any]]]) -> dict[str, Any]:
    layout: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}},
    }
    rows: list[list[str]] = [[]]
    used = 0
    components = [("MARKDOWN-notes", 12, None)] + [
        (f"CHART-{chart_id}", c["width"], (chart_id, c)) for chart_id, c in charts
    ]
    for component_id, width, _ in components:
        if used + width > 12:
            rows.append([])
            used = 0
        rows[-1].append(component_id)
        used += width
    for index, children in enumerate(rows, start=1):
        row_id = f"ROW-{index}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": children,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for child in children:
            parents = ["ROOT_ID", "GRID_ID", row_id]
            if child == "MARKDOWN-notes":
                layout[child] = {
                    "type": "MARKDOWN",
                    "id": child,
                    "children": [],
                    "parents": parents,
                    "meta": {"width": 12, "height": 12, "code": NOTES[title]},
                }
                continue
            chart_id, chart = next(v for k, w, v in components if k == child)  # type: ignore[misc]
            layout[child] = {
                "type": "CHART",
                "id": child,
                "children": [],
                "parents": parents,
                "meta": {
                    "chartId": chart_id,
                    "width": chart["width"],
                    "height": 50,
                    "sliceName": chart["name"],
                },
            }
    return layout


def main() -> int:
    api = Api()
    databases = api.find("database", "database_name", DATABASE)
    database_id = (
        databases[0]["id"]
        if databases
        else api.call(
            "POST",
            "/api/v1/database/",
            {"database_name": DATABASE, "sqlalchemy_uri": URI, "expose_in_sqllab": True},
        )["id"]
    )

    datasets: dict[str, int] = {}
    for name in sorted({c["dataset"] for charts in CHARTS.values() for c in charts}):
        found = [d for d in api.find("dataset", "table_name", name) if d.get("schema") == "gold"]
        datasets[name] = (
            found[0]["id"]
            if found
            else api.call(
                "POST",
                "/api/v1/dataset/",
                {"database": database_id, "schema": "gold", "table_name": name},
            )["id"]
        )

    dashboard_ids = []
    for title, charts in CHARTS.items():
        for old in api.find("dashboard", "dashboard_title", title):
            for chart in api.call("GET", f"/api/v1/dashboard/{old['id']}/charts")["result"]:
                api.call("DELETE", f"/api/v1/chart/{chart['id']}")
            api.call("DELETE", f"/api/v1/dashboard/{old['id']}")
        dashboard_id = api.call(
            "POST",
            "/api/v1/dashboard/",
            {
                "dashboard_title": title,
                "slug": title.lower().replace(" ", "-"),
                "published": True,
            },
        )["id"]
        created = []
        for chart in charts:
            dataset_id = datasets[chart["dataset"]]
            fd = form_data(chart, dataset_id)
            chart_id = api.call(
                "POST",
                "/api/v1/chart/",
                {
                    "slice_name": chart["name"],
                    "viz_type": chart["viz"],
                    "datasource_id": dataset_id,
                    "datasource_type": "table",
                    "params": json.dumps(fd),
                    "query_context": json.dumps(query_context(chart, dataset_id, fd)),
                    "dashboards": [dashboard_id],
                },
            )["id"]
            created.append((chart_id, chart))
        api.call(
            "PUT",
            f"/api/v1/dashboard/{dashboard_id}",
            {"position_json": json.dumps(position(title, created))},
        )
        dashboard_ids.append(dashboard_id)
        print(f"superset: built '{title}' with {len(created)} charts")

    rison = "!(" + ",".join(str(i) for i in dashboard_ids) + ")"
    archive = api.call("GET", f"/api/v1/dashboard/export/?q={urllib.parse.quote(rison)}", raw=True)
    shutil.rmtree(BUNDLE, ignore_errors=True)
    BUNDLE.mkdir(parents=True)
    with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
        for member in bundle.infolist():
            relative = Path(
                *Path(member.filename).parts[1:]
            )  # drop the dashboard_export_<ts>/ root
            if member.is_dir() or not relative.parts:
                continue
            target = BUNDLE / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(bundle.read(member))
    print(f"superset: exported {len(dashboard_ids)} dashboards to {BUNDLE.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
