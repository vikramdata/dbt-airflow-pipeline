FROM apache/airflow:2.10.5-python3.11

# dbt lives in its own virtualenv so its dependencies never clash with Airflow's.
RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres==1.9.0
