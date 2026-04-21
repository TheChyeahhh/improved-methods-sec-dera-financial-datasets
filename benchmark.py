"""
Benchmark: Pandas vs Polars on SEC DERA num.txt (3M+ rows)

Run after downloading data with Populate_Data.py:
    python benchmark.py
"""

import time
import pandas as pd
import polars as pl

FILE = "examples/data/2022q1/num.txt"


def bench_pandas() -> float:
    start = time.time()
    df = pd.read_csv(FILE, sep="\t", low_memory=False)
    _ = df[df["tag"] == "Revenues"].groupby("adsh")["value"].sum()
    return time.time() - start


def bench_polars() -> float:
    start = time.time()
    _ = (
        pl.scan_csv(FILE, separator="\t")
        .filter(pl.col("tag") == "Revenues")
        .group_by("adsh")
        .agg(pl.col("value").sum())
        .collect()
    )
    return time.time() - start


if __name__ == "__main__":
    print("Running Pandas benchmark...")
    pandas_time = bench_pandas()

    print("Running Polars benchmark...")
    polars_time = bench_polars()

    print(f"\nPandas:  {pandas_time:.2f}s")
    print(f"Polars:  {polars_time:.2f}s")
    print(f"Speedup: {pandas_time / polars_time:.1f}x faster")
