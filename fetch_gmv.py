"""
Fetch daily GMV data from Databricks for a given country.

Usage:
    python3 fetch_gmv.py --country cz --start 2025-09-01 --output CZ/daily_gmv.csv

Requires DATABRICKS_TOKEN_COMMON env var (or .env in ~/databricks-setup/).
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime


def fetch(country_code: str, start_date: str, output_path: str):
    sys.path.insert(0, str(Path.home() / "databricks-setup"))
    from dbx import DBX

    today = datetime.now().strftime("%Y-%m-%d")
    print(f"Fetching daily GMV for {country_code.upper()} from {start_date} to {today}...")

    with DBX("common") as dbx:
        df = dbx.query(f"""
            SELECT
                metric_timestamp_partition AS date,
                ROUND(SUM(total_gmv_before_discounts_eur), 0) AS daily_gmv_eur,
                SUM(delivered_orders_count) AS delivered_orders
            FROM ng_delivery_spark.fact_delivery_country_daily
            WHERE country_code = '{country_code}'
              AND delivery_vertical IN ('food', 'store_3p_ent', 'store_3p_mm_smb')
              AND metric_timestamp_partition >= '{start_date}'
            GROUP BY metric_timestamp_partition
            ORDER BY date
        """)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"  Saved {len(df)} rows to {out}")
    print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    print(f"  Avg daily GMV: {df['daily_gmv_eur'].mean():,.0f} EUR")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch daily GMV from Databricks")
    parser.add_argument("--country", required=True, help="Country code (e.g. cz, ee)")
    parser.add_argument("--start", required=True, help="Start date (e.g. 2025-09-01)")
    parser.add_argument("--output", required=True, help="Output CSV path")
    args = parser.parse_args()
    fetch(args.country, args.start, args.output)
