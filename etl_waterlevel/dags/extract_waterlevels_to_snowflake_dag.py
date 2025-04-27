from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
import pendulum
import os
import pandas as pd
import logging

# Import your Snowflake writer function
from waterlevel.db.snowflake_writer import load_csv_to_snowflake_via_copy
from waterlevel.extract.raw_waterlevels import fetch_waterlevels  # optional

logger = logging.getLogger(__name__)
os.makedirs("/opt/airflow/data", exist_ok=True)

# Step 1: Save the CSV (reuse logic from earlier DAG)
def save_to_csv():
    try:
        waterlevels = fetch_waterlevels()
        df = pd.DataFrame(waterlevels)
        df = df.dropna(subset=['stationName'])
        df.to_csv("/opt/airflow/data/raw_waterlevels.csv", index=False)
        logger.info("Successfully saved Raw Water Levels!")
    except Exception as e:
        logger.error(f"❌ Failed to fetch/save water levels: {str(e)}")
        raise

# Step 2: Load to Snowflake
def load_to_snowflake():
    try:
        load_csv_to_snowflake_via_copy("/opt/airflow/data/raw_waterlevels.csv")
    except Exception as e:
        logger.error(f"❌ Failed to load into Snowflake: {str(e)}")
        raise

default_args = {
    'start_date': datetime(2024, 1, 1, tzinfo=pendulum.timezone("Asia/Kuala_Lumpur")),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG("extract_waterlevels_to_snowflake",
         default_args=default_args,
         description="Extract and load Selangor water levels into Snowflake",
         schedule_interval="5 * * * *",  # every hour at :15
         catchup=False,
         tags=['extract', 'snowflake']
) as dag:

    extract_task = PythonOperator(
        task_id="extract_raw_waterlevels",
        python_callable=save_to_csv,
    )

    load_snowflake_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    extract_task >> load_snowflake_task
