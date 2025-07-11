from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from common.api import fetch_brewery_data
from common.transforms import transform_to_silver, aggregate_gold

with DAG("brewery_pipeline_all_layers", start_date=datetime(2025,7,1), schedule="@daily") as dag:
    
    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=fetch_brewery_data
    )

    silver_task = PythonOperator(
        task_id="silver_transformation",
        python_callable=transform_to_silver
    )

    gold_task = PythonOperator(
        task_id="gold_aggregation",
        python_callable=aggregate_gold
    )

    bronze_task >> silver_task >> gold_task
