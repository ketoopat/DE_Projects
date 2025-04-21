from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import subprocess
import pendulum
import logging

logger = logging.getLogger(__name__)

def run_dbt_model(**context):
    """Execute the fact_waterlevel dbt model to transform raw water level data"""
    try:
        dag_run = context.get('dag_run')
        if dag_run and dag_run.conf:
             triggered_by = dag_run.conf.get('triggered_by')
             logger.info(f"This DAG was triggered by: {triggered_by}")

        dbt_project_dir = "/usr/app"
        result = subprocess.run(
            ["dbt", "run", "--models", "fact_waterlevel"],
            cwd=dbt_project_dir,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"Successfully ran DBT model: {result.stdout[:100]}") # show 
        return result.stdout
    except Exception as e:
        logger.error(f"Error running DBT model: {str(e)}")
        raise

default_args = {
    'start_date': datetime(2024, 1, 1, tzinfo=pendulum.timezone("Asia/Kuala_Lumpur")),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    "transform_waterlevels_daily",
    default_args=default_args,
    description="Transform water level data using DBT",
    schedule_interval=None,  # Runs 25 minutes after extraction (allowing time for extraction to complete)
    catchup=False,
    tags=['transform', 'dbt']
) as dag:
    
    # Optional: Wait for the extraction DAG to complete
    # wait_for_extraction = ExternalTaskSensor(
    #     task_id="wait_for_extraction",
    #     external_dag_id="extract_waterlevels_daily",
    #     external_task_id="load_to_postgres",
    #     timeout=600,  # 10 minutes
    #     mode="reschedule",  # Keep checking until the dependency is met
    #     poke_interval=60,  # Check every minute
    # )

        transform_task = PythonOperator(
        task_id="transform_with_dbt",
        python_callable=run_dbt_model,
        provide_context=True,  # Pass context to access the triggering information
    )