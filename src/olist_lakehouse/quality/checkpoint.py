"""The bronze batch checkpoint: is bronze fit to build silver from? (Great Expectations)

    python -m olist_lakehouse.quality.checkpoint [--plant-failure]

Where the streaming gate (gate.py) judges each ROW as it lands, this judges bronze as a WHOLE
before a dbt build, on a profile with one row per landed source (a CDC table or the
clickstream):

    landed            rows in its bronze table
    quarantined       rows the gate sent to quarantine
    quarantine_share  quarantined / (landed + quarantined)
    gate_gx_agree     did the gate and GX agree on every batch of this source?

Expectations (suite `bronze_profile`):
    every expected source has landed rows          (a source that went silent)
    quarantine_share <= 5%                         (chaos runs at 2%; much more means trouble)
    gate_gx_agree is true everywhere               (the two implementations still agree)
    --plant-failure adds quarantine_share <= 0     (fails on purpose; the M4-S2 red team)

Results go to a file-backed GX project (GX_ROOT) and its Data Docs site, served by the
`data-docs` container at http://localhost:28089. Exit 1 when any expectation fails, so the
Airflow task fails and dbt does not run.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

GX_ROOT = Path(os.environ.get("GX_ROOT", "/opt/airflow/gx"))
SUITE = "bronze_profile"
EXPECTED_SOURCES = (
    "customers",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "product_category_translation",
    "products",
    "sellers",
    "zip_geolocation",
    "clickstream",
)

PROFILE_SQL = """
WITH landed AS (
    SELECT source_table AS source, count(*) AS n FROM lake.bronze.cdc_events GROUP BY 1
    UNION ALL
    SELECT 'clickstream', count(*) FROM lake.bronze.clickstream_events
),
quarantined AS (
    SELECT coalesce(source_table, 'clickstream') AS source, count(*) AS n
    FROM lake.bronze.quarantine GROUP BY 1
),
agreement AS (
    SELECT source, bool_and(coalesce(agree, true)) AS agree
    FROM lake.bronze.quality_metrics WHERE kind = 'gate' GROUP BY 1
)
SELECT
    l.source,
    l.n AS landed,
    coalesce(q.n, 0) AS quarantined,
    coalesce(a.agree, true) AS gate_gx_agree
FROM landed AS l
LEFT JOIN quarantined AS q ON q.source = l.source
LEFT JOIN agreement AS a ON a.source = CASE WHEN l.source = 'clickstream' THEN 'clickstream' ELSE 'cdc' END
"""


def profile() -> pd.DataFrame:
    import trino

    cursor = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "127.0.0.1"),
        port=int(os.environ.get("TRINO_PORT", "28090")),
        user="gx-checkpoint",
    ).cursor()
    cursor.execute(PROFILE_SQL)
    frame = pd.DataFrame(
        cursor.fetchall(), columns=["source", "landed", "quarantined", "gate_gx_agree"]
    )
    frame["quarantine_share"] = frame["quarantined"] / (frame["landed"] + frame["quarantined"])
    return frame


def expectations(plant_failure: bool) -> list[Any]:
    found = [
        gxe.ExpectColumnDistinctValuesToContainSet(
            column="source", value_set=list(EXPECTED_SOURCES)
        ),
        gxe.ExpectColumnValuesToBeBetween(column="landed", min_value=1),
        gxe.ExpectColumnValuesToBeBetween(column="quarantine_share", min_value=0, max_value=0.05),
        gxe.ExpectColumnValuesToBeInSet(column="gate_gx_agree", value_set=[True]),
    ]
    if plant_failure:
        found.append(
            gxe.ExpectColumnValuesToBeBetween(
                column="quarantine_share",
                max_value=0,
                meta={"planted": "M4-S2 red team: quarantine must be empty (it never is)"},
            )
        )
    return found


def run(frame: pd.DataFrame, *, plant_failure: bool = False) -> tuple[bool, list[str]]:
    context = gx.get_context(mode="file", project_root_dir=str(GX_ROOT))
    source = context.data_sources.add_or_update_pandas(name="bronze")
    try:
        asset = source.get_asset("profile")
    except LookupError:
        asset = source.add_dataframe_asset(name="profile")
    try:
        batch_definition = asset.get_batch_definition("whole")
    except (KeyError, LookupError):
        batch_definition = asset.add_batch_definition_whole_dataframe("whole")

    suite = gx.ExpectationSuite(name=SUITE, expectations=expectations(plant_failure))
    suite = context.suites.add_or_update(suite)
    validation = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name="bronze_profile", data=batch_definition, suite=suite)
    )
    checkpoint = context.checkpoints.add_or_update(
        gx.Checkpoint(
            name="bronze_before_dbt",
            validation_definitions=[validation],
            actions=[gx.checkpoint.UpdateDataDocsAction(name="update_data_docs")],
            result_format={"result_format": "COMPLETE"},
        )
    )
    result = checkpoint.run(batch_parameters={"dataframe": frame})
    failed = [
        f"{r.expectation_config.type} {r.expectation_config.kwargs.get('column', '')}".strip()
        for run in result.run_results.values()
        for r in run.results
        if not r.success
    ]
    return bool(result.success), failed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--plant-failure", action="store_true")
    args = parser.parse_args(argv)
    frame = profile()
    print(frame.to_string(index=False))
    success, failed = run(frame, plant_failure=args.plant_failure)
    if not success:
        print(f"checkpoint: FAILED {failed}; Data Docs: http://localhost:28089", file=sys.stderr)
        return 1
    print("checkpoint: bronze is fit for dbt; Data Docs: http://localhost:28089")
    return 0


if __name__ == "__main__":
    sys.exit(main())
