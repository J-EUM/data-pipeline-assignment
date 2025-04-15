from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from cell2info.tasks import (
    download_cell2info,
    extract_gz_to_tsv,
    load_cells_to_sqlite,
    load_synonyms_to_sqlite,
    load_taxonomy_to_sqlite,
    parse_and_prepare,
)

with DAG(dag_id="cell2info", catchup=False, max_active_runs=1) as dag:
    # init_db = SQLExecuteQueryOperator(
    #     task_id="init_db",
    #     sql="create_tables.sql",
    #     autocommit=True,
    #     conn_id="sqlite_conn",
    #     split_statements=True,
    # )

    download_data = PythonOperator(
        task_id="download_data",
        python_callable=download_cell2info,
    )

    extract_gz_to_tsv = PythonOperator(
        task_id="extract_gz_to_tsv", python_callable=extract_gz_to_tsv
    )

    parse_and_prepare = PythonOperator(
        task_id="parse_and_prepare", python_callable=parse_and_prepare
    )

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

    download_data >> extract_gz_to_tsv >> parse_and_prepare >> load
