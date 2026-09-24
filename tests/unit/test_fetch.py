"""fetch verifies before it promotes, so a refusal never touches the destination."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from olist_lakehouse import fetch
from olist_lakehouse.contracts import ContractError

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "olist"


@pytest.fixture
def fake_kaggle(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    """Replace the Kaggle download with a copy of the fixtures; record each call."""
    calls: list[Path] = []

    def download(staging: Path) -> None:
        calls.append(staging)
        shutil.copytree(FIXTURES, staging, dirs_exist_ok=True)

    monkeypatch.setattr(fetch, "download", download)
    return calls


def test_manifest_round_trips(tmp_path: Path) -> None:
    manifest = tmp_path / "olist.sha256"
    fetch.write_manifest(manifest, FIXTURES)
    pins = fetch.read_manifest(manifest)
    assert pins["olist_orders_dataset.csv"] == fetch.sha256(FIXTURES / "olist_orders_dataset.csv")
    assert manifest.read_bytes().endswith(b"\n")
    assert b"\r" not in manifest.read_bytes()


def test_sha256sum_binary_marker_is_tolerated(tmp_path: Path) -> None:
    manifest = tmp_path / "m"
    manifest.write_text("ABCDEF *olist_orders_dataset.csv\n# comment\n\n")
    assert fetch.read_manifest(manifest) == {"olist_orders_dataset.csv": "abcdef"}


def test_first_run_needs_an_explicit_write_manifest(
    tmp_path: Path, fake_kaggle: list[Path]
) -> None:
    with pytest.raises(fetch.FetchError) as refused:
        fetch.run(raw=tmp_path / "raw", manifest=tmp_path / "m", write=False)
    assert refused.value.code == "FETCH-NO-MANIFEST"
    assert fake_kaggle == []


def test_write_then_rerun_skips_the_download(tmp_path: Path, fake_kaggle: list[Path]) -> None:
    raw, manifest = tmp_path / "raw", tmp_path / "olist.sha256"
    first = fetch.run(raw=raw, manifest=manifest, write=True)
    assert manifest.is_file() and len(fake_kaggle) == 1
    assert not (tmp_path / ".staging-olist").exists()

    second = fetch.run(raw=raw, manifest=manifest, write=False)
    assert len(fake_kaggle) == 1, "a verified destination must not download again"
    assert second == first


def test_a_hash_mismatch_refuses_and_leaves_the_destination_alone(
    tmp_path: Path, fake_kaggle: list[Path]
) -> None:
    raw, manifest = tmp_path / "raw", tmp_path / "olist.sha256"
    fetch.write_manifest(manifest, FIXTURES)
    manifest.write_text(
        manifest.read_text().replace(fetch.sha256(FIXTURES / "olist_sellers_dataset.csv"), "0" * 64)
    )
    with pytest.raises(fetch.FetchError) as refused:
        fetch.run(raw=raw, manifest=manifest, write=False)
    assert refused.value.code == "FETCH-HASH-MISMATCH"
    assert not raw.exists()
    assert not (tmp_path / ".staging-olist").exists()


def test_a_contract_refusal_writes_nothing(
    tmp_path: Path, fake_kaggle: list[Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_download(staging: Path) -> None:
        shutil.copytree(FIXTURES, staging, dirs_exist_ok=True)
        orders = staging / "olist_orders_dataset.csv"
        orders.write_bytes(orders.read_bytes().replace(b'"order_status"', b'"status"', 1))

    monkeypatch.setattr(fetch, "download", broken_download)
    raw, manifest = tmp_path / "raw", tmp_path / "olist.sha256"
    with pytest.raises(ContractError) as refused:
        fetch.run(raw=raw, manifest=manifest, write=True)
    assert refused.value.code == "CONTRACT-MISSING-COLUMN"
    assert not raw.exists()
    assert not manifest.exists()


def test_main_reports_a_refusal_with_its_code(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code = fetch.main(["--raw-dir", str(tmp_path / "raw"), "--manifest", str(tmp_path / "none")])
    assert code == 1
    assert "FETCH-NO-MANIFEST" in capsys.readouterr().err
