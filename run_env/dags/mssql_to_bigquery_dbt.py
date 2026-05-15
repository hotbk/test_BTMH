from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import bigquery


MSSQL_CONN_ID = "src_mssql"
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BIGQUERY_LOCATION = os.environ.get("BIGQUERY_LOCATION", "US")
DEFAULT_DESTINATION_DATASET = os.environ.get("MSSQL_BQ_DATASET", "btmh_stg_mssql")
DBT_PROJECT_DIR = "/opt/airflow/dags/dbt/btmh_dmt"
DBT_BIN = "/home/airflow/.local/bin/dbt"

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

DEFAULT_TABLES = [
    {
        "source_database": "vietdb",
        "source_schema": "dbo",
        "source_table": "Results_BN1",
        "destination_dataset": DEFAULT_DESTINATION_DATASET,
        "destination_table": "results_bn1",
        "write_disposition": "WRITE_TRUNCATE",
        "batch_size": 50000,
    }
]


def _load_table_config() -> list[dict[str, Any]]:
    raw = Variable.get("mssql_to_bq_tables", default_var=json.dumps(DEFAULT_TABLES))
    tables = json.loads(raw)
    if not isinstance(tables, list) or not tables:
        raise ValueError("Airflow Variable mssql_to_bq_tables must be a non-empty JSON list")
    return tables


def _quote_mssql_identifier(identifier: str) -> str:
    if not IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Unsafe MSSQL identifier: {identifier!r}")
    return f"[{identifier}]"


def _qualified_mssql_table(table_config: dict[str, Any]) -> str:
    parts = [
        table_config.get("source_database"),
        table_config.get("source_schema", "dbo"),
        table_config["source_table"],
    ]
    return ".".join(_quote_mssql_identifier(part) for part in parts if part)


def _load_mssql_table_to_bigquery(table_config: dict[str, Any]) -> None:
    source_table = table_config["source_table"]
    destination_dataset = table_config.get("destination_dataset", DEFAULT_DESTINATION_DATASET)
    destination_table = table_config.get("destination_table", source_table.lower())
    write_disposition = table_config.get("write_disposition", "WRITE_TRUNCATE")
    batch_size = int(table_config.get("batch_size", 50000))

    table_id = f"{GCP_PROJECT_ID}.{destination_dataset}.{destination_table}"
    qualified_source = _qualified_mssql_table(table_config)

    bq_client = bigquery.Client(project=GCP_PROJECT_ID, location=BIGQUERY_LOCATION)
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{destination_dataset}")
    dataset_ref.location = BIGQUERY_LOCATION
    bq_client.create_dataset(dataset_ref, exists_ok=True)

    hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    loaded_rows = 0
    current_write_disposition = write_disposition

    with hook.get_conn() as conn:
        for chunk in pd.read_sql_query(f"select * from {qualified_source}", conn, chunksize=batch_size):
            chunk["_btmh_ingested_at"] = pd.Timestamp.utcnow()
            chunk["_btmh_source_table"] = qualified_source

            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition=current_write_disposition,
            )
            load_job = bq_client.load_table_from_dataframe(
                chunk,
                table_id,
                job_config=job_config,
            )
            load_job.result()

            loaded_rows += len(chunk)
            current_write_disposition = "WRITE_APPEND"

    print(f"Loaded {loaded_rows} rows from {qualified_source} to {table_id}")


def load_mssql_tables_to_bigquery() -> None:
    for table_config in _load_table_config():
        _load_mssql_table_to_bigquery(table_config)


with DAG(
    dag_id="mssql_to_bigquery_dbt",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False,
    tags=["mssql", "bigquery", "dbt"],
) as dag:
    load_raw_tables = PythonOperator(
        task_id="load_mssql_tables_to_bigquery",
        python_callable=load_mssql_tables_to_bigquery,
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_mssql_models",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} run --profiles-dir . --select tag:mssql_ingestion"
        ),
    )

    load_raw_tables >> run_dbt_models
