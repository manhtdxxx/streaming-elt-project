from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_run_and_test',
    default_args=default_args,
    description='Run dbt models and tests',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['dbt'],
) as dag:

    # dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --profiles-dir /home/airflow/.dbt --project-dir /opt/airflow/dags',
    )

    # dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --profiles-dir /home/airflow/.dbt --project-dir /opt/airflow/dags',
    )

    dbt_run >> dbt_test
