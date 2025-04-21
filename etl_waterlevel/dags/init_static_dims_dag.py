from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging
import pendulum

logger = logging.getLogger(__name__)

def run_static_dims():
    """Run all static dimension tables tagged with 'static'"""
    try:
        result = subprocess.run(
            ["dbt", "run", "--select", "tag:static"],
            cwd="/usr/app",  # must match your mounted dbt folder
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("Static dimension models ran successfully.")
        logger.debug(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error("DBT run for static dimensions failed.")
        logger.error(e.stderr)
        raise

default_args = {
    'start_date': datetime(2024, 1, 1, tzinfo=pendulum.timezone("Asia/Kuala_Lumpur")),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    "static_dims",
    default_args=default_args,
    description="Run static dimension tables with tag:static using DBT",
    schedule_interval=None,  # only run manually
    catchup=False,
    tags=["static", "dbt"]
) as dag:

    load_static_dims = PythonOperator(
        task_id="run_static_tables",
        python_callable=run_static_dims,
    )
