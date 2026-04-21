"""
Benchmark: Pandas vs Polars (TSV) vs Polars (Parquet)
Runs on SEC DERA num.txt — 3M+ rows.

Run after downloading data:
    python Populate_Data.py
    python benchmark.py
"""

import time
import os
import pandas as pd
import polars as pl

TSV_FILE     = "examples/data/2022q1/num.txt"
PARQUET_FILE = "examples/data/2022q1/num.parquet"


def bench_pandas() -> float:
    start = time.time()
    df = pd.read_csv(TSV_FILE, sep="\t", low_memory=False)
    _ = df[df["tag"] == "Revenues"].groupby("adsh")["value"].sum()
    return time.time() - start


def bench_polars_tsv() -> float:
    start = time.time()
    _ = (
        pl.scan_csv(TSV_FILE, separator="\t")
        .filter(pl.col("tag") == "Revenues")
        .group_by("adsh")
        .agg(pl.col("value").sum())
        .collect()
    )
    return time.time() - start


def bench_polars_parquet() -> float:
    start = time.time()
    _ = (
        pl.scan_parquet(PARQUET_FILE)
        .filter(pl.col("tag") == "Revenues")
        .group_by("adsh")
        .agg(pl.col("value").sum())
        .collect()
    )
    return time.time() - start


if __name__ == "__main__":
    sep = "─" * 45

    print("Running Pandas (TSV)...")
    pandas_time = bench_pandas()

    print("Running Polars (TSV)...")
    polars_tsv_time = bench_polars_tsv()

    parquet_available = os.path.exists(PARQUET_FILE)
    if parquet_available:
        print("Running Polars (Parquet)...")
        polars_parquet_time = bench_polars_parquet()
    else:
        print("Parquet file not found — run Populate_Data.py to generate it.")

    print(f"\n{sep}")
    print(f"{'Method':<22} {'Time':>6}   {'vs Pandas':>10}")
    print(sep)
    print(f"{'Pandas  (TSV)':<22} {pandas_time:>5.2f}s   {'baseline':>10}")
    print(f"{'Polars  (TSV)':<22} {polars_tsv_time:>5.2f}s   {pandas_time/polars_tsv_time:>9.1f}x")
    if parquet_available:
        print(f"{'Polars  (Parquet)':<22} {polars_parquet_time:>5.2f}s   {pandas_time/polars_parquet_time:>9.1f}x")
    print(sep)
