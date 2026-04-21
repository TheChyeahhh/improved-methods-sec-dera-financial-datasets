# -*- coding: utf-8 -*-
"""
Downloads SEC DERA financial statement data sets, converts to Parquet,
and supports incremental updates (only re-downloads what has changed).

@author: U.S. Securities and Exchange Commission (original)
Upgraded to Polars + Parquet + incremental updates by TheChyeahhh

Parquet conversion and incremental update strategy inspired by a conversation
with Ian Gow and his independent R implementation of the same pipeline:
https://github.com/iangow/notes
"""

import polars as pl
import requests
import zipfile
import io
import os
import json
from datetime import datetime, timezone

DATASETS = [
    ("https://www.sec.gov/files/dera/data/financial-statement-data-sets/2022q1.zip",       "examples/data/2022q1"),
    ("https://www.sec.gov/files/dera/data/financial-statement-data-sets/2022q2.zip",       "examples/data/2022q2"),
    ("https://www.sec.gov/files/dera/data/financial-statement-data-sets/2022q3.zip",       "examples/data/2022q3"),
    ("https://www.sec.gov/files/dera/data/financial-statement-data-sets/2022q4.zip",       "examples/data/2022q4"),
    ("https://www.sec.gov/files/dera/data/financial-statement-notes-data-sets/2024q1_notes.zip", "examples/data/2024q1_notes"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research-tool; contact: transplant003@yahoo.com)"
}

METADATA_FILE = "examples/data/.metadata.json"


def load_metadata() -> dict:
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE) as f:
            return json.load(f)
    return {}


def save_metadata(metadata: dict) -> None:
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2)


def remote_last_modified(url: str) -> str:
    """HEAD request to check Last-Modified without downloading the full file."""
    try:
        r = requests.head(url, headers=HEADERS, timeout=10)
        return r.headers.get("Last-Modified", "")
    except Exception:
        return ""


def needs_update(url: str, metadata: dict) -> bool:
    """Return True if the remote file is newer than our last download."""
    remote = remote_last_modified(url)
    if not remote:
        return True  # can't verify — re-download to be safe
    return metadata.get(url, {}).get("last_modified") != remote


def download_and_extract(url: str, output_dir: str) -> str:
    """Download ZIP, extract contents, return the Last-Modified header value."""
    print(f"  Downloading: {url}")
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    zipfile.ZipFile(io.BytesIO(response.content)).extractall(output_dir)
    print(f"  Extracted to: {output_dir}")
    return response.headers.get("Last-Modified", "")


def convert_to_parquet(output_dir: str) -> None:
    """Convert every TSV/TXT in output_dir to a Parquet file beside it."""
    for fname in os.listdir(output_dir):
        if fname.endswith((".txt", ".tsv")):
            tsv_path = os.path.join(output_dir, fname)
            parquet_path = os.path.splitext(tsv_path)[0] + ".parquet"
            print(f"  Converting {fname} → Parquet ...")
            (
                pl.scan_csv(tsv_path, separator="\t", infer_schema_length=10000)
                .collect()
                .write_parquet(parquet_path)
            )
    print(f"  Parquet conversion complete: {output_dir}\n")


def load_parquet(filepath: str) -> pl.LazyFrame:
    """Return a lazy frame over a Parquet file — nothing reads until .collect()."""
    return pl.scan_parquet(filepath)


if __name__ == "__main__":
    metadata = load_metadata()
    updated = 0

    for url, output_dir in DATASETS:
        os.makedirs(output_dir, exist_ok=True)
        label = os.path.basename(url)

        if not needs_update(url, metadata):
            print(f"Up to date — skipping: {label}")
            continue

        print(f"\n[{label}]")
        last_modified = download_and_extract(url, output_dir)
        convert_to_parquet(output_dir)

        metadata[url] = {
            "last_modified": last_modified,
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "output_dir": output_dir,
        }
        save_metadata(metadata)
        updated += 1

    print(f"\n{'─' * 45}")
    print(f"Done. {updated} dataset(s) updated. Parquet files ready.")
    print(f"{'─' * 45}")
