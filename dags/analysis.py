from __future__ import annotations
from datetime import datetime, timedelta
import os
import shutil
from airflow import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import Error as DatabaseError
import csv
from typing import Iterable
import pandas as pd
import json
import numpy as np


# Paths & DB targets for data
OUTPUT_DIR = "/opt/airflow/data"
BTC_CSV = os.path.join(OUTPUT_DIR, "coin_Bitcoin.csv")
ETH_CSV = os.path.join(OUTPUT_DIR, "coin_Ethereum.csv")
SCHEMA = "week8_demo"
TARGET_TABLE = "crypto_prices_merged"

default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
    tags=["crypto", "csv", "postgres"],
) as dag:

    @task(multiple_outputs=True)
    def verify_inputs(btc_path: str = BTC_CSV, eth_path: str = ETH_CSV) -> dict:
        if not os.path.exists(btc_path):
            raise FileNotFoundError(f"Missing {btc_path}")
        if not os.path.exists(eth_path):
            raise FileNotFoundError(f"Missing {eth_path}")
        return {"btc_path": btc_path, "eth_path": eth_path}

    paths = verify_inputs()

    @task()
    def fetch_bitcoin(source_path: str, output_dir: str = OUTPUT_DIR) -> str:
        os.makedirs(output_dir, exist_ok=True)
        dest = os.path.join(output_dir, "bitcoin.csv")
        shutil.copyfile(source_path, dest)
        print(f"Bitcoin data copied to {dest}")
        return dest

    @task()
    def fetch_ethereum(source_path: str, output_dir: str = OUTPUT_DIR) -> str:
        os.makedirs(output_dir, exist_ok=True)
        dest = os.path.join(output_dir, "ethereum.csv")
        shutil.copyfile(source_path, dest)
        print(f"Ethereum data copied to {dest}")
        return dest


    @task()
    def merge_csvs(btc_path: str, eth_path: str, output_dir: str = OUTPUT_DIR) -> str:

        os.makedirs(output_dir, exist_ok=True)
        out_path = os.path.join(output_dir, "crypto_merged.csv")

        def _read_and_standardize(path: str) -> pd.DataFrame:
            df = pd.read_csv(path)
            df.columns = [c.strip().lower() for c in df.columns]
            # try common date-like column names
            date_col = next((c for c in ("date", "timestamp", "time") if c in df.columns), None)
            if date_col is None:
                raise ValueError(f"Could not find a date/timestamp column in {path}")
            df["date"] = pd.to_datetime(df[date_col], errors="coerce")
            df = df.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
            return df

        btc = _read_and_standardize(btc_path)
        eth = _read_and_standardize(eth_path)

        # pick a small, typical set of numeric columns if present
        keep = ["open", "high", "low", "close", "volume"]
        btc_ren = {c: f"btc_{c}" for c in keep if c in btc.columns}
        eth_ren = {c: f"eth_{c}" for c in keep if c in eth.columns}

        btc_small = btc[["date"] + list(btc_ren.keys())].rename(columns=btc_ren)
        eth_small = eth[["date"] + list(eth_ren.keys())].rename(columns=eth_ren)

        merged = pd.merge(btc_small, eth_small, on="date", how="inner")
        merged.to_csv(out_path, index=False)

        print(f"Merged CSV saved to {out_path} with {len(merged)} rows")
        return out_path

    @task()
    def load_csv_to_pg(
        conn_id: str,
        csv_path: str,
        schema: str = SCHEMA,
        table: str = TARGET_TABLE,
        append: bool = True,
    ) -> int:
        import csv  # local import to keep globals tidy

        # Read the CSV
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or []
            rows = [tuple((r.get(col, "") or None) for col in fieldnames) for r in reader]

        if not fieldnames:
            print("CSV had no header; nothing to create.")
            return 0
        if not rows:
            print("No rows found in CSV; nothing to insert.")
            return 0

        # SQL prep
        create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        create_table = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                {', '.join([f'{col} TEXT' for col in fieldnames])}
            );
        """
        delete_rows = f"DELETE FROM {schema}.{table};" if not append else None
        insert_sql = f"""
            INSERT INTO {schema}.{table} ({', '.join(fieldnames)})
            VALUES ({', '.join(['%s' for _ in fieldnames])});
        """

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(create_schema)
                cur.execute(create_table)
                if delete_rows:
                    cur.execute(delete_rows)
                    print(f"Cleared existing rows in {schema}.{table}")
                cur.executemany(insert_sql, rows)
                conn.commit()
            inserted = len(rows)
            print(f"Inserted {inserted} rows into {schema}.{table}")
            return inserted
        except DatabaseError as e:
            print(f"Database error: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    @task()
    def clear_folder(
        folder_path: str = OUTPUT_DIR,
        keep_files: Iterable[str] | None = None,   # files to preserve
    ) -> None:
        """
        Delete all files and subdirectories inside `folder_path`,
        except those explicitly listed in `keep_files`.
        Keeps the folder itself.
        """
        import os
        import shutil

        if not os.path.exists(folder_path):
            print(f"Folder {folder_path} does not exist.")
            return

        # Default: keep the two original datasets
        keep_set = {os.path.abspath(BTC_CSV), os.path.abspath(ETH_CSV)}
        if keep_files:
            keep_set |= {os.path.abspath(p) for p in keep_files}

        for name in os.listdir(folder_path):
            path = os.path.abspath(os.path.join(folder_path, name))

            # Skip protected files
            if path in keep_set:
                print(f"Keeping: {path}")
                continue

            try:
                if os.path.isfile(path) or os.path.islink(path):
                    os.remove(path)
                    print(f"Removed file: {path}")
                elif os.path.isdir(path):
                    shutil.rmtree(path)
                    print(f"Removed directory: {path}")
            except Exception as e:
                print(f"Failed to delete {path}: {e}")

        print("Clean process completed!")

    @task()
    def analyze_prices(
        conn_id: str,
        schema: str = SCHEMA,
        table: str = TARGET_TABLE,
        output_dir: str = OUTPUT_DIR,
    ) -> list[str]:
        """Read from Postgres, compute correlation & beta, and (optionally) plot."""
        # 1) Read from DB
        hook = PostgresHook(postgres_conn_id=conn_id)
        with hook.get_conn() as conn:
            df = pd.read_sql(f'SELECT * FROM {schema}.{table} ORDER BY "date"', conn)

        # Normalize column names just in case
        df.columns = [c.strip().lower() for c in df.columns]
        # Find close columns we created during merge
        btc_close = next((c for c in df.columns if c == "btc_close"), None)
        eth_close = next((c for c in df.columns if c == "eth_close"), None)
        if not btc_close or not eth_close:
            raise ValueError("Could not find btc_close/eth_close columns in database table.")

        # 2) Compute daily percent returns
        df["btc_ret"] = pd.to_numeric(df[btc_close], errors="coerce").pct_change()
        df["eth_ret"] = pd.to_numeric(df[eth_close], errors="coerce").pct_change()
        ret = df[["btc_ret", "eth_ret"]].dropna()

        corr = float(ret.corr().loc["btc_ret", "eth_ret"]) if not ret.empty else float("nan")

        # Simple linear regression: eth_ret = alpha + beta * btc_ret
        if len(ret) >= 2:
            beta, alpha = np.polyfit(ret["btc_ret"].values, ret["eth_ret"].values, 1)
            beta = float(beta)
            alpha = float(alpha)
        else:
            beta, alpha = float("nan"), float("nan")

        os.makedirs(output_dir, exist_ok=True)

        # 3) Save summary JSON
        summary_path = os.path.join(output_dir, "analysis_summary.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "rows_read": int(len(df)),
                    "rows_used_for_returns": int(len(ret)),
                    "corr_eth_vs_btc_returns": corr,
                    "beta_eth_on_btc_returns": beta,
                    "alpha_eth_on_btc_returns": alpha,
                },
                f,
                indent=2,
            )

        # 4) Optional plot (skip gracefully if matplotlib isn't installed)
        plot_path = ""
        try:
            import matplotlib.pyplot as plt

            fig = plt.figure(figsize=(9, 4.5))
            plt.plot(pd.to_datetime(df["date"]), pd.to_numeric(df[btc_close], errors="coerce"), label="BTC Close")
            plt.plot(pd.to_datetime(df["date"]), pd.to_numeric(df[eth_close], errors="coerce"), label="ETH Close")
            plt.title("Bitcoin vs Ethereum Close")
            plt.xlabel("Date")
            plt.ylabel("Price")
            plt.legend()
            plot_path = os.path.join(output_dir, "crypto_prices.png")
            plt.tight_layout()
            plt.savefig(plot_path)
            plt.close(fig)
        except Exception as e:
            print(f"[analyze_prices] Skipping plot: {e}")

        # Return only existing files; clear_folder will keep these
        keep = [p for p in [summary_path, plot_path] if p and os.path.exists(p)]
        print(f"[analyze_prices] Outputs: {keep}")
        return keep

    # wire it up
    bitcoin_file = fetch_bitcoin(paths["btc_path"])
    ethereum_file = fetch_ethereum(paths["eth_path"])
    merged_path = merge_csvs(bitcoin_file, ethereum_file)

    # wiring with your existing task outputs
    # (you already have: bitcoin_file, ethereum_file, merged_path)
    load_to_database = load_csv_to_pg(
        conn_id="Postgres",
        csv_path=merged_path,
        table=TARGET_TABLE,   # optional; default already equals TARGET_TABLE
    )


    analysis_outputs = analyze_prices(conn_id="Postgres")
    clean_folder = clear_folder(keep_files=analysis_outputs)
    load_to_database >> analysis_outputs >> clean_folder