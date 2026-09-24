"""The bronze checkpoint's verdicts, on hand-built profiles, in a throwaway GX project."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from olist_lakehouse.quality import checkpoint


def profile(**overrides: object) -> pd.DataFrame:
    rows = [
        {"source": s, "landed": 1000, "quarantined": 20, "gate_gx_agree": True}
        for s in checkpoint.EXPECTED_SOURCES
    ]
    frame = pd.DataFrame(rows)
    for column, value in overrides.items():
        frame.loc[0, column] = value
    frame["quarantine_share"] = frame["quarantined"] / (frame["landed"] + frame["quarantined"])
    return frame


@pytest.fixture(autouse=True)
def gx_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setattr(checkpoint, "GX_ROOT", tmp_path / "gx")
    return tmp_path / "gx"


def test_a_healthy_bronze_passes_and_publishes_data_docs(gx_root: Path) -> None:
    success, failed = checkpoint.run(profile())
    assert (success, failed) == (True, [])
    assert list(gx_root.rglob("data_docs/local_site/index.html"))


@pytest.mark.parametrize(
    ("overrides", "failing_column"),
    [
        ({"quarantined": 400}, "quarantine_share"),  # 400 / 1400 = 29%
        ({"landed": 0, "quarantined": 0}, "landed"),
        ({"gate_gx_agree": False}, "gate_gx_agree"),
    ],
)
def test_an_unhealthy_bronze_fails_on_the_right_expectation(
    overrides: dict[str, object], failing_column: str
) -> None:
    success, failed = checkpoint.run(profile(**overrides).fillna(0))
    assert not success
    assert any(failing_column in f for f in failed), failed


def test_a_missing_source_fails() -> None:
    success, failed = checkpoint.run(profile().iloc[1:])
    assert not success
    assert any("distinct_values_to_contain_set" in f for f in failed), failed


def test_the_planted_failure_fails_a_healthy_bronze() -> None:
    success, failed = checkpoint.run(profile(), plant_failure=True)
    assert not success
    assert failed == ["expect_column_values_to_be_between quarantine_share"]
