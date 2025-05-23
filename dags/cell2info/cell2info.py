from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from cell2info.tasks import (
    delete_temp_directory,
    download_cell2info,
    extract_gz_to_tsv,
    load_cells_to_sqlite,
    load_synonyms_to_sqlite,
    load_taxonomy_to_sqlite,
    parse_and_prepare,
)

default_args = {
    "start_date": days_ago(2),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    dag_id="cell2info", catchup=False, max_active_runs=1, default_args=default_args
) as dag:
    download_data = PythonOperator(
        task_id="download_data",
        python_callable=download_cell2info,
    )

    with TaskGroup(group_id="prepare_data") as transform:
        extract_gz_to_tsv = PythonOperator(
            task_id="extract_gz_to_tsv", python_callable=extract_gz_to_tsv
        )

        parse_and_prepare = PythonOperator(
            task_id="parse_and_prepare", python_callable=parse_and_prepare
        )

        extract_gz_to_tsv >> parse_and_prepare

    with TaskGroup(group_id="load_to_sqlite") as load:
        load_taxonomy = PythonOperator(
            task_id="load_taxonomy_to_sqlite", python_callable=load_taxonomy_to_sqlite
        )

        load_cells = PythonOperator(
            task_id="load_cells_to_sqlite", python_callable=load_cells_to_sqlite
        )

        load_synonyms = PythonOperator(
            task_id="load_synonyms_to_sqlite", python_callable=load_synonyms_to_sqlite
        )

        load_taxonomy >> load_cells >> load_synonyms

    cleanup = PythonOperator(
        task_id="delete_temp_directory", python_callable=delete_temp_directory
    )

    download_data >> transform >> load >> cleanup
