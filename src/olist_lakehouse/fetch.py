"""Download the Olist dataset from Kaggle, verify it, and promote it into data/raw/olist.

The order is what makes this safe to rerun: download into a staging directory next to the
destination, check every sha256 against the committed manifest, check the contract, and only
then move the files into place. A refusal at any step leaves the destination untouched.

    FETCH-DOWNLOAD-FAILED   the Kaggle CLI exited non-zero
    FETCH-MISSING-FILE      the download lacks a file the manifest pins
    FETCH-HASH-MISMATCH     a file's bytes differ from the manifest
    FETCH-NO-MANIFEST       no manifest yet; run once with --write-manifest
    CONTRACT-*              see olist_lakehouse.contracts

Usage:
    python -m olist_lakehouse.fetch                    # download (or reuse) + verify + contract
    python -m olist_lakehouse.fetch --write-manifest   # first time only: pin what was downloaded
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
from pathlib import Path

from olist_lakehouse import settings
from olist_lakehouse.contracts import COLUMNS, ContractError, FileReport, check_directory


class FetchError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def read_manifest(path: Path) -> dict[str, str]:
    """Parse `sha256sum` format: `<hex>  <name>` (a `*` binary marker is tolerated)."""
    pins: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        digest, name = line.split(maxsplit=1)
        pins[name.lstrip("*")] = digest.lower()
    return pins


def write_manifest(path: Path, directory: Path) -> None:
    lines = [f"{sha256(directory / name)}  {name}" for name in sorted(COLUMNS)]
    header = f"# sha256 of the Olist CSVs from kaggle.com/datasets/{settings.KAGGLE_DATASET}"
    path.write_bytes(("\n".join([header, *lines]) + "\n").encode("utf-8"))


def verify(directory: Path, pins: dict[str, str]) -> None:
    for name, expected in sorted(pins.items()):
        path = directory / name
        if not path.is_file():
            raise FetchError("FETCH-MISSING-FILE", f"{name} is pinned but was not downloaded")
        actual = sha256(path)
        if actual != expected:
            raise FetchError(
                "FETCH-HASH-MISMATCH", f"{name}: sha256 {actual}, manifest pins {expected}"
            )


def download(staging: Path) -> None:
    command = [
        settings.KAGGLE_BIN,
        "datasets",
        "download",
        settings.KAGGLE_DATASET,
        "-p",
        str(staging),
        "--unzip",
        "--quiet",
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise FetchError(
            "FETCH-DOWNLOAD-FAILED", f"exit {result.returncode}: {result.stderr.strip()[-500:]}"
        )


def already_present(raw: Path, pins: dict[str, str]) -> bool:
    try:
        verify(raw, pins)
    except FetchError:
        return False
    return True


def promote(staging: Path, raw: Path) -> None:
    raw.mkdir(parents=True, exist_ok=True)
    for name in sorted(COLUMNS):
        (staging / name).replace(raw / name)


def run(*, raw: Path, manifest: Path, write: bool) -> list[FileReport]:
    pins = read_manifest(manifest) if manifest.is_file() else None
    if pins is None and not write:
        raise FetchError(
            "FETCH-NO-MANIFEST", f"{manifest} does not exist; run once with --write-manifest"
        )

    if pins is not None and already_present(raw, pins):
        print(f"fetch: {raw} already matches the manifest; skipping the download")
        return check_directory(raw)

    staging = raw.parent / ".staging-olist"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    try:
        download(staging)
        if pins is not None:
            verify(staging, pins)
        reports = check_directory(staging)
        if write:
            write_manifest(manifest, staging)
            print(f"fetch: wrote {manifest}")
        promote(staging, raw)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return reports


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--raw-dir", type=Path, default=settings.RAW_DIR)
    parser.add_argument("--manifest", type=Path, default=settings.MANIFEST)
    parser.add_argument("--write-manifest", action="store_true")
    args = parser.parse_args(argv)

    try:
        reports = run(raw=args.raw_dir, manifest=args.manifest, write=args.write_manifest)
    except (FetchError, ContractError) as exc:
        print(f"fetch: REFUSED {exc}", file=sys.stderr)
        return 1

    for report in reports:
        print(f"  {report.file:<40} {report.rows:>9,} rows  {report.columns} columns")
    print(f"fetch: {len(reports)} files verified and within contract in {args.raw_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
