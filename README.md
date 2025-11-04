# data-engineering-airflow-project
# Crypto Price Pipeline (Airflow)

## Overview
An Apache Airflow DAG pipeline that ingests two related CSV datasets (Bitcoin and Ethereum), applies transformations, merges them, loads the result into Postgres, performs a lightweight analysis, and cleans up intermediates.

## Steps
1. **Ingestion (parallel):**
   - `fetch_bitcoin` and `fetch_ethereum` copy `coin_Bitcoin.csv` and `coin_Ethereum.csv` into the container’s `/opt/airflow/data/`.

2. **Transform + Merge:**
   - `merge_csvs` standardizes column names, parses timestamps, selects common OHLCV columns, and merges on `date`.
   - Output: `/opt/airflow/data/crypto_merged.csv`.

3. **Load to Postgres:**
   - `load_csv_to_pg` creates schema/table (`week8_demo.crypto_prices_merged`) and inserts all rows.

4. **Analysis:**
   - `analyze_prices` reads the merged table from Postgres, computes daily returns, correlation (ETH vs BTC), and a simple linear-regression beta (ETH on BTC returns).
   - Saves:
     - `analysis_summary.json` (metrics),
     - `crypto_prices.png` (optional, if `matplotlib` is installed).

5. **Cleanup:**
   - `clear_folder` deletes intermediates under `/opt/airflow/data/` but **keeps** the two original CSVs and the analysis outputs.

## Scheduling
- The DAG uses `@once` to run a single time on demand (scheduling can be swapped for cron if desired).

## Notes & Lessons
- Airflow connections are case-sensitive; my Postgres connection ID is `Postgres`.
- Using the TaskFlow API avoided passing data via XCom; only file paths and small lists are exchanged.
- For reproducibility, containerized setup (Docker Compose) ensures the same Airflow + provider versions locally.
