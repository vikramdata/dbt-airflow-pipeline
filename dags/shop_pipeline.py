"""
Daily e-commerce pipeline: generate data -> check freshness -> snapshot -> transform -> test -> docs.

catchup=True with a start_date 30 days back means the first time you switch the DAG on,
Airflow backfills a month of history one day at a time (max_active_runs=1 keeps them in
order, which matters for the SCD2 snapshots).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"
DBT = "/home/airflow/dbt_venv/bin/dbt --no-use-colors"
DBT_CMD = f"cd {DBT_DIR} && {DBT}"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="shop_pipeline",
    description="Synthetic e-commerce ELT with dbt",
    default_args=default_args,
    start_date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=30),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["dbt", "elt"],
) as dag:

    extract_load = BashOperator(
        task_id="extract_load_raw",
        bash_command="python /opt/airflow/scripts/generate_data.py --date {{ ds }}",
    )

    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_CMD} source freshness",
    )

    snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_CMD} snapshot",
    )

    run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging",
    )

    test_staging = BashOperator(
        task_id="dbt_test_staging",
        # cautious = only run tests whose upstream models are ALL in this selection,
        # so tests that also touch the marts wait for the dbt_test_marts task.
        bash_command=f"{DBT_CMD} test --select staging source:* path:snapshots --indirect-selection cautious",
    )

    run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD} run --select intermediate marts",
    )

    test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"{DBT_CMD} test --select intermediate marts",
    )

    docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT_CMD} docs generate",
    )

    (
        extract_load
        >> source_freshness
        >> snapshot
        >> run_staging
        >> test_staging
        >> run_marts
        >> test_marts
        >> docs
    )
