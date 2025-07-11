import pandas as pd
import json
from pathlib import Path

def transform_to_silver():
    # Load latest JSON file from bronze
    bronze_dir = Path("/opt/airflow/data/bronze")
    latest_file = sorted(bronze_dir.glob("*.json"))[-1]
    with open(latest_file) as f:
        data = json.load(f)
    df = pd.DataFrame(data)

    # Transform and partition by state
    df = df[df['state'].notnull()]
    df['state'] = df['state'].fillna('unknown')

    silver_dir = Path("/opt/airflow/data/silver")
    silver_dir.mkdir(parents=True, exist_ok=True)
    for state, group in df.groupby("state"):
        group.to_parquet(silver_dir / f"breweries_{state}.parquet", index=False)

def aggregate_gold():
    silver_dir = Path("/opt/airflow/data/silver")
    dfs = [pd.read_parquet(p) for p in silver_dir.glob("*.parquet")]
    full_df = pd.concat(dfs)

    agg_df = full_df.groupby(["state", "brewery_type"]).size().reset_index(name="count")
    gold_dir = Path("/opt/airflow/data/gold")
    gold_dir.mkdir(parents=True, exist_ok=True)

    agg_df.to_csv(gold_dir / f"breweries_aggregated.csv", index=False)
