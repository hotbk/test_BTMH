from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime

def test_mssql():
    hook = MsSqlHook(mssql_conn_id="src_mssql")
    result = hook.get_records("SELECT @@version")
    print("✓ Connection success!")
    print(result)

with DAG(
    dag_id="test_mssql_connection",
    start_date=datetime(2026, 5, 1),
    schedule=None,
    catchup=False
) as dag:
    test = PythonOperator(task_id="test", python_callable=test_mssql)
