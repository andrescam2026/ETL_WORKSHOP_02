import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator

import etl_funciones as etl

argumentos_por_defecto = {
    "owner": "Carlos Camacho",
    "retries": 0,
}

with DAG(
    dag_id="workshop2_spotify_grammys",
    description="ETL: Spotify (CSV) + Grammys (BD) -> merge -> SQLite + CSV local",
    default_args=argumentos_por_defecto,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "workshop2"],
) as dag:

    t_read_csv = PythonOperator(task_id="read_csv", python_callable=etl.read_csv)
    t_validate_csv = PythonOperator(task_id="validate_csv", python_callable=etl.validate_csv)
    t_transform_csv = PythonOperator(task_id="transform_csv", python_callable=etl.transform_csv)

    t_read_db = PythonOperator(task_id="read_db", python_callable=etl.read_db)
    t_transform_db = PythonOperator(task_id="transform_db", python_callable=etl.transform_db)

    t_merge = PythonOperator(task_id="merge", python_callable=etl.merge)
    t_load = PythonOperator(task_id="load", python_callable=etl.load)
    t_store = PythonOperator(task_id="store", python_callable=etl.store)

    t_read_csv >> t_validate_csv >> t_transform_csv
    t_read_db >> t_transform_db
    [t_transform_csv, t_transform_db] >> t_merge >> t_load >> t_store
