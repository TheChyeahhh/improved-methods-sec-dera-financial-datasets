# Python for DERA Financial Datasets — Polars Edition

> A modernized fork of the official SEC DERA Python repository,
> upgraded from Pandas to Polars for dramatically faster performance
> and lower memory usage on large XBRL financial datasets.

## Original Repository

This project is a fork and upgrade of the official SEC DERA repository:
https://github.com/sec-gov/python-for-dera-financial-datasets

All original work belongs to the U.S. Securities and Exchange Commission (SEC).
This fork is provided for educational and analytical improvement purposes.

---

## What Changed

| Feature     | Original (Pandas)          | This Fork (Polars)              |
|-------------|---------------------------|----------------------------------|
| Execution   | Eager (loads all RAM)      | Lazy (loads only what's needed) |
| Threading   | Single-threaded            | Multi-threaded                  |
| Speed       | Baseline                   | 10–100x faster                  |
| Memory      | High                       | Significantly lower             |
| SQL Support | No                         | Yes (DuckDB integration)        |

---

## What's Included

- All original DERA notebooks — rewritten in Polars
- `Populate_Data.py` — upgraded data downloader with lazy loading
- `benchmark.py` — Pandas vs Polars speed comparison
- `examples/duckdb_query_example.py` — SQL interface for power users

---

## Requirements

- Python 3.x
- polars
- duckdb
- jupyter
- matplotlib
- seaborn
- requests
- openpyxl
- pyarrow
- xlsxwriter

---

## Setup

```bash
git clone https://github.com/TheChyeahhh/python-for-dera-financial-datasets
cd python-for-dera-financial-datasets
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows
pip install -r requirements.txt
python Populate_Data.py
jupyter notebook
```

---

## Examples

| Example | Description |
| :-- | :-- |
| **1_Read_And_Analyze_Submissions.ipynb** | Read 2022 Q1 submissions into a Polars DataFrame. Covers filtering, grouping, descriptive stats, and pyplot visualizations. |
| **2_Read_Multiple_Data_Sets.ipynb** | Concatenate four quarters of submissions, derive a Month column, and plot monthly filing statistics. |
| **3_Find_Numeric_Facts.ipynb** | Join submissions with numeric facts to compare airline revenue and income tax across multiple years. |
| **4_Find_Dimensional_Facts.ipynb** | Three-way join of submissions, numeric facts, and dimensions to find dimensional revenue data for Delta Air Lines. |
| **5_Find_Narrative_Text_Facts.ipynb** | Join submissions with text facts. Find narrative disclosures and export 12(g) securities to Excel. |
| **6_Find_Custom_Facts.ipynb** | Join submissions, numeric facts, and tags to find custom (company-defined) XBRL facts for Amazon. |

---

## Run the Benchmark

```bash
python benchmark.py
```

Expected output (results vary by machine):
```
Pandas:  12.40s
Polars:   0.95s
Speedup: 13.1x faster
```

---

## Data Source

SEC DERA Financial Statement Data Sets:
https://www.sec.gov/dera/data/financial-statement-data-sets

SEC DERA Financial Statement and Notes Data Sets:
https://www.sec.gov/dera/data/financial-statement-and-notes-data-set

---

*Fork maintained by [TheChyeahhh](https://github.com/TheChyeahhh). Not affiliated with the SEC. Educational use only.*
