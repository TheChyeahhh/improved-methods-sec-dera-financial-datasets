# -*- coding: utf-8 -*-
"""
Downloads SEC DERA financial statement data sets and loads them with Polars.

@author: U.S. Securities and Exchange Commission (original)
Upgraded to Polars by TheChyeahhh
"""

import polars as pl
import requests
import zipfile
import io
import os

QUARTERS = [
    ("https://www.sec.gov/files/dera/data/financial-statement-data-sets/2022q1.zip", "examples/data/2022q1"),
    ("https://www.sec.gov/files/dera/data/financial-statement-data-sets/2022q2.zip", "examples/data/2022q2"),
    ("https://www.sec.gov/files/dera/data/financial-statement-data-sets/2022q3.zip", "examples/data/2022q3"),
    ("https://www.sec.gov/files/dera/data/financial-statement-data-sets/2022q4.zip", "examples/data/2022q4"),
    ("https://www.sec.gov/files/dera/data/financial-statement-notes-data-sets/2024q1_notes.zip", "examples/data/2024q1_notes"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; research-tool; contact: transplant003@yahoo.com)"
}


def download_and_extract(url: str, output_dir: str) -> None:
    print(f"Downloading: {url}")
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(response.content))
    z.extractall(output_dir)
    print(f"Extracted to {output_dir}")


def load_quarter(filepath: str) -> pl.LazyFrame:
    """Return a lazy frame — nothing is loaded until .collect() is called."""
    return pl.scan_csv(filepath, separator="\t", infer_schema_length=10000)


if __name__ == "__main__":
    for url, output_dir in QUARTERS:
        os.makedirs(output_dir, exist_ok=True)
        download_and_extract(url, output_dir)

    print("\nAll data downloaded. Example lazy load:")
    lf = load_quarter("examples/data/2022q1/num.txt")
    print(lf.schema)
