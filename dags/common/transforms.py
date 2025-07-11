import pandas as pd
import json
import logging
from pathlib import Path
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

def transform_to_silver():
    try:
        # Load latest JSON file from bronze
        bronze_dir = Path("/opt/airflow/data/bronze")
        latest_file = sorted(bronze_dir.glob("*.json"))[-1]
        if not latest_file:
            raise AirflowException("No raw files found in bronze layer.")
        
        with open(latest_file) as f:
            logger.info(f"Reading file {f} in bronze layer")
            data = json.load(f)
            
        df = pd.DataFrame(data)

        # Transform and partition by state
        logger.info("Transforming file to silver layer")
        df = df[df['state'].notnull()]
        df['state'] = df['state'].fillna('unknown')

        silver_dir = Path("/opt/airflow/data/silver")
        silver_dir.mkdir(parents=True, exist_ok=True)
        for state, group in df.groupby("state"):
            group.to_parquet(silver_dir / f"breweries_{state}.parquet", index=False)

        logger.info("Transformed file saved in silver folder")

    except Exception as e:
        logger.exception("Failed to transform data to silver layer")
        raise AirflowException(f"Silver transformation failed: {e}")

def aggregate_gold():
    try:
        silver_dir = Path("/opt/airflow/data/silver")
        logger.info("Reading files in silver layer")
        dfs = [pd.read_parquet(p) for p in silver_dir.glob("*.parquet")]
        if not dfs:
            raise AirflowException("No silver files found to aggregate.")
        
        full_df = pd.concat(dfs)

        if full_df.empty:
            raise AirflowException("No data to aggregate in silver layer.")
        
        logger.info("Aggregating data to gold layer")
        agg_df = full_df.groupby(["state", "brewery_type"]).size().reset_index(name="count")
        gold_dir = Path("/opt/airflow/data/gold")
        gold_dir.mkdir(parents=True, exist_ok=True)

        agg_df.to_csv(gold_dir / f"breweries_aggregated.csv", index=False)
        logger.info("Aggregated data saved in gold folder")

    except Exception as e:
        logger.exception("Failed to aggregate data to gold layer")
        raise AirflowException(f"Gold aggregation failed: {e}")
