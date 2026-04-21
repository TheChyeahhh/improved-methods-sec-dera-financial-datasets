"""
DuckDB SQL interface for SEC DERA financial data.

Queries run directly on the TSV files — no loading into memory required.
Run after downloading data with Populate_Data.py:
    python examples/duckdb_query_example.py
"""

import duckdb

conn = duckdb.connect()

print("=== Top 20 Revenue Filers (2022 Q1) ===")
result = conn.execute("""
    SELECT adsh, SUM(value) AS total_revenue
    FROM read_csv_auto('examples/data/2022q1/num.txt', delim='\t')
    WHERE tag = 'Revenues'
    GROUP BY adsh
    ORDER BY total_revenue DESC
    LIMIT 20
""").df()
print(result)

print("\n=== Form Filing Counts (2022 Q1) ===")
result2 = conn.execute("""
    SELECT form, COUNT(*) AS count
    FROM read_csv_auto('examples/data/2022q1/sub.txt', delim='\t')
    GROUP BY form
    ORDER BY count DESC
    LIMIT 10
""").df()
print(result2)
