from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # Note the import path changed in newer Airflow versions
from waterlevel.extract.raw_waterlevels import fetch_waterlevels
from waterlevel.db.pg_writer import load_csv_toPostgres
import pandas as pd
import os
import pendulum
import logging

logger = logging.getLogger(__name__)
os.makedirs("/opt/airflow/data", exist_ok=True)


def save_to_csv():
    try:
        waterlevels = fetch_waterlevels()
        df = pd.DataFrame(waterlevels)
        df = df.dropna(subset=['stationName'])
        df.to_csv("/opt/airflow/data/raw_waterlevels.csv", index=False)
        logger.info("Successfully saved Raw Water Levels!")
    except Exception as e:
        logger.error(f"Failed to fetch or save water levels: {str(e)}")
        raise

def load_csv_task():
    try:
        load_csv_toPostgres("/opt/airflow/data/raw_waterlevels.csv")
    except Exception as e:
        logger.error(f"Failed to load into Postgres: {str(e)}")

default_args = {
    'start_date': datetime(2024, 1, 1, tzinfo=pendulum.timezone("Asia/Kuala_Lumpur")),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG("extract_waterlevels_daily",
         default_args=default_args,
         description="Extract Selangor hourly water level data",
         schedule_interval="5 * * * *",  # Runs at the 5th minute of every hour (e.g., 08:05, 09:05)
         catchup=False,  # <- moved here
         tags=['extract']
)as dag:

        
    extract_task = PythonOperator(
        task_id="extract_raw_waterlevels",
        python_callable=save_to_csv,
    )

    load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_csv_task,
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="transform_waterlevels_daily",
        wait_for_completion=False,
        reset_dag_run=True,
        conf={"triggered_by": "extract_waterlevels_daily"}
    )

    extract_task >> load_task >> trigger_transform