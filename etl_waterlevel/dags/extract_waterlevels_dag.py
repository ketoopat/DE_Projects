from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from waterlevel.extract.raw_waterlevels import fetch_waterlevels
from waterlevel.db.pg_writer import load_csv_toPostgres
import pandas as pd
import os
import pendulum

os.makedirs("/opt/airflow/data", exist_ok=True)


def save_to_csv():
    waterlevels = fetch_waterlevels()
    df = pd.DataFrame(waterlevels)
    df = df.dropna(subset=['stationName'])
    df.to_csv("/opt/airflow/data/raw_waterlevels.csv", index=False)
    print("Successfully saved Raw Water Levels!")

def load_csv_task():
    load_csv_toPostgres("/opt/airflow/data/raw_waterlevels.csv")

default_args = {
    'start_date': datetime(2024, 1, 1, tzinfo=pendulum.timezone("Asia/Kuala_Lumpur")),
}

with DAG("extract_waterlevels_daily",
         default_args=default_args,
         description="Extract Selangor daily water level data",
         schedule_interval="0 * * * *",  # Runs at the start of every hour (e.g., 08:00, 09:00)
         catchup=False,  # <- moved here
         tags=['extract']) as dag:

        
    extract_task = PythonOperator(
        task_id="extract_raw_waterlevels",
        python_callable=save_to_csv,
    )

    load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_csv_task,
    )

    extract_task >> load_task

