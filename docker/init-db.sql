-- One Postgres container, two databases:
--   airflow   -> Airflow's own metadata
--   warehouse -> the raw data and everything dbt builds
create database airflow;
create database warehouse;
