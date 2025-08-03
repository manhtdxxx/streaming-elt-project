from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# define path in container
profiles_dir = "/home/airflow/.dbt"
project_dir = "/opt/airflow/dags/nyc_taxi"
target_env = "prod"

# common config for dag
default_args = {
    'owner': 'manh',
    'depends_on_past': False,  # set True if previous day failed, then skip today
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dbt_run_and_test',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=timedelta(minutes=1),
    catchup=False,  # set True to run from start date to now
    description='Run dbt models and tests',
    tags=['dbt'],
) as dag:

    # dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            f"dbt run --profiles-dir {profiles_dir} --project-dir {project_dir} --target {target_env}"
        ),
    )

    # dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            f"dbt test --profiles-dir {profiles_dir} --project-dir {project_dir} --target {target_env}"
        ),
    )

    dbt_run >> dbt_test
