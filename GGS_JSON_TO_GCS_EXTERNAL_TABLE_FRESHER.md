# Huong Dan Task `extract_and_upload` JSON API -> GCS Parquet -> BigQuery External Table

Tai lieu nay danh cho fresher khi tao luong lay du lieu GGS tu URL JSON theo ngay chay cua Airflow.

Muc tieu cua task:

```text
Airflow run date `ds`
    -> goi URL JSON
    -> parse payload lay list record
    -> map cot raw sang cot BigQuery
    -> ep kieu numeric/date/string
    -> ghi Parquet tam vao /tmp
    -> upload Parquet len GCS
    -> sinh DDL external table
    -> BigQueryInsertJobOperator chay DDL refresh staging
```

## 1. Nguyen tac bat buoc

Dung ngay chay cua Airflow, khong dung `datetime.today()`.

Trong Airflow, `ds` co format:

```text
YYYY-MM-DD
```

Vi du neu DAG run cho ngay `2026-05-15`, task phai ghi dung partition:

```text
gs://<GCS_BUCKET>/<GCS_BASE_PREFIX>/dt=2026-05-15/source=ggs/...
```

Khong hard-code ngay trong code.

## 2. Cau hinh can co

Nen khai bao cac bien o dau file DAG:

```python
import os

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BIGQUERY_LOCATION = os.environ.get("BIGQUERY_LOCATION", "US")

GCS_BUCKET = os.environ["GCS_BUCKET"]
GCS_BASE_PREFIX = os.environ.get("GCS_BASE_PREFIX", "raw")

STAGING_DATASET = os.environ.get("GGS_STAGING_DATASET", "btmh_stg_ggs")
STAGING_TABLE = os.environ.get("GGS_STAGING_TABLE", "ggs_records")

GGS_JSON_URL = os.environ["GGS_JSON_URL"]
```

Neu URL can token, lay token tu Airflow Variable/Connection, khong commit token vao git.

## 3. Thu vien can import

Trong `requirements.txt` hien da co:

```text
apache-airflow-providers-google
google-cloud-storage
google-cloud-bigquery
pandas
pyarrow
```

Trong DAG can import:

```python
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage
```

Neu container bao loi `ModuleNotFoundError: requests`, them `requests` vao `requirements.txt` va rebuild image.

## 4. Dinh nghia schema va mapping cot

Fresher phai tao mapping ro rang: raw column ben JSON -> ten cot chuan BigQuery.

Vi du:

```python
COLUMN_MAPPING = {
    "OrderId": "order_id",
    "CustomerCode": "customer_code",
    "OrderDate": "order_date",
    "Amount": "amount",
    "Note": "note",
}

NUMERIC_COLUMNS = ["amount"]
DATE_COLUMNS = ["order_date"]
STRING_COLUMNS = ["order_id", "customer_code", "note"]
```

Ten cot BigQuery nen dung snake_case, khong co dau cach, khong dau tieng Viet.

## 5. Task `extract_and_upload`

Task nay chi lam viec extract, transform nhe, ghi file va upload len GCS. Task khong chay DDL.

Skeleton chuan:

```python
def extract_and_upload(ds: str, **context) -> str:
    url = GGS_JSON_URL
    params = {
        "date": ds,
    }

    response = requests.get(url, params=params, timeout=120)
    response.raise_for_status()
    payload = response.json()

    records = _extract_records(payload)
    df = _normalize_records(records, ds)

    local_path = Path(f"/tmp/ggs_records_{ds}.parquet")
    df.to_parquet(local_path, index=False, engine="pyarrow")

    object_name = (
        f"{GCS_BASE_PREFIX.rstrip('/')}/"
        f"dt={ds}/"
        f"source=ggs/"
        f"ggs_records_{ds}.parquet"
    )

    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(local_path))

    gcs_uri = f"gs://{GCS_BUCKET}/{object_name}"
    print(f"Uploaded {len(df)} rows to {gcs_uri}")
    return gcs_uri
```

Operator phai truyen `ds` bang Jinja:

```python
extract_task = PythonOperator(
    task_id="extract_and_upload",
    python_callable=extract_and_upload,
    op_kwargs={"ds": "{{ ds }}"},
)
```

## 6. Parse payload lay list record

Khong assume payload luc nao cung la list. Viet helper rieng de doc dung format API.

Vi du API co dang:

```json
{
  "data": [
    {
      "OrderId": "SO001",
      "Amount": "120000"
    }
  ]
}
```

Helper:

```python
def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        records = payload.get("data") or payload.get("records") or payload.get("items") or []
    else:
        raise ValueError(f"Unexpected JSON payload type: {type(payload).__name__}")

    if not isinstance(records, list):
        raise ValueError("JSON records must be a list")

    return records
```

Neu API thuc te dung key khac `data`, sua helper theo API thuc te va comment ro.

## 7. Map cot va ep kieu

Viet helper rieng de bien JSON raw thanh dataframe dung schema staging.

```python
def _normalize_records(records: list[dict[str, Any]], ds: str) -> pd.DataFrame:
    df = pd.DataFrame(records)

    if df.empty:
        df = pd.DataFrame(columns=list(COLUMN_MAPPING.values()))
    else:
        df = df.rename(columns=COLUMN_MAPPING)

    expected_columns = list(COLUMN_MAPPING.values())
    for column in expected_columns:
        if column not in df.columns:
            df[column] = None

    df = df[expected_columns]

    for column in NUMERIC_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    for column in DATE_COLUMNS:
        df[column] = pd.to_datetime(df[column], errors="coerce").dt.date

    for column in STRING_COLUMNS:
        df[column] = df[column].astype("string")

    df["_btmh_run_date"] = pd.to_datetime(ds).date()
    df["_btmh_ingested_at"] = pd.Timestamp.utcnow()

    return df
```

Luu y: `_btmh_run_date` phai set bang `ds`, khong lay tu data source.

## 8. Path GCS phai dung format

Format bat buoc:

```text
gs://<GCS_BUCKET>/<GCS_BASE_PREFIX>/dt=<ds>/source=ggs/....
```

Vi du:

```text
gs://btmh-data/raw/ggs/dt=2026-05-15/source=ggs/ggs_records_2026-05-15.parquet
```

Neu `GCS_BASE_PREFIX=raw/ggs`, object name la:

```text
raw/ggs/dt=2026-05-15/source=ggs/ggs_records_2026-05-15.parquet
```

Khong them slash dau object name.

## 9. Sinh DDL external table

External table tro vao dung partition folder theo `ds`.

Vi du DDL:

```python
def build_external_table_ddl(ds: str) -> str:
    source_uri = (
        f"gs://{GCS_BUCKET}/"
        f"{GCS_BASE_PREFIX.rstrip('/')}/"
        f"dt={ds}/source=ggs/*.parquet"
    )

    table_id = f"`{GCP_PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}`"

    return f"""
    CREATE OR REPLACE EXTERNAL TABLE {table_id}
    OPTIONS (
      format = 'PARQUET',
      uris = ['{source_uri}']
    )
    """
```

Voi external table, `CREATE OR REPLACE` se refresh metadata cua bang staging de tro vao file Parquet moi.

## 10. BigQueryInsertJobOperator chay DDL

Dung `BigQueryInsertJobOperator`, khong dung Python client cho DDL neu DAG da co operator BigQuery.

```python
refresh_external_table = BigQueryInsertJobOperator(
    task_id="refresh_external_table",
    location=BIGQUERY_LOCATION,
    configuration={
        "query": {
            "query": """
            CREATE OR REPLACE EXTERNAL TABLE `{{ var.value.gcp_project_id }}.btmh_stg_ggs.ggs_records`
            OPTIONS (
              format = 'PARQUET',
              uris = ['gs://{{ var.value.gcs_bucket }}/raw/ggs/dt={{ ds }}/source=ggs/*.parquet']
            )
            """,
            "useLegacySql": False,
        }
    },
)
```

Neu project/bucket dang lay tu environment variable, nen tao string bang Python de tranh sai `Variable`.

Vi du:

```python
external_table_ddl = f"""
CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{STAGING_DATASET}.{STAGING_TABLE}`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://{GCS_BUCKET}/{GCS_BASE_PREFIX.rstrip("/")}/dt={{{{ ds }}}}/source=ggs/*.parquet']
)
"""

refresh_external_table = BigQueryInsertJobOperator(
    task_id="refresh_external_table",
    location=BIGQUERY_LOCATION,
    configuration={
        "query": {
            "query": external_table_ddl,
            "useLegacySql": False,
        }
    },
)
```

## 11. Thu tu task trong DAG

Thu tu toi thieu:

```text
extract_and_upload
        |
        v
refresh_external_table
```

Code:

```python
extract_task >> refresh_external_table
```

Neu sau do co dbt:

```text
extract_and_upload
        |
        v
refresh_external_table
        |
        v
run_dbt_models
```

## 12. Checklist truoc khi merge

Kiem tra code:

- `extract_and_upload` nhan `ds` tu Airflow, khong dung ngay local.
- URL JSON co timeout va co `raise_for_status()`.
- Payload duoc parse thanh `list[dict]`.
- Mapping cot raw -> cot BigQuery nam trong constant ro rang.
- Cot numeric dung `pd.to_numeric(..., errors="coerce")`.
- Cot date dung `pd.to_datetime(..., errors="coerce").dt.date`.
- Cot string dung pandas string dtype hoac object string.
- File Parquet ghi vao `/tmp`.
- GCS path co `dt=<ds>/source=ggs/`.
- DDL external table dung `CREATE OR REPLACE EXTERNAL TABLE`.
- BigQueryInsertJobOperator dung `useLegacySql: False`.
- Task order la `extract_and_upload >> refresh_external_table`.

Kiem tra tren Airflow UI:

- DAG parse thanh cong, khong bi import error.
- Chay manual DAG voi ngay test.
- Log task `extract_and_upload` co dong `Uploaded ... rows to gs://...`.
- GCS co file `.parquet` dung folder `dt=<ds>/source=ggs/`.
- Task `refresh_external_table` thanh cong.
- Query BigQuery staging table tra ve du lieu.

## 13. Loi thuong gap

Loi sai ngay:

```text
Dung datetime.today() lam path GCS
```

Cach sua:

```text
Dung `ds` Airflow truyen vao task.
```

Loi sai path:

```text
gs://bucket/raw/source=ggs/dt=2026-05-15/...
```

Cach sua:

```text
gs://bucket/raw/dt=2026-05-15/source=ggs/...
```

Loi BigQuery khong doc duoc file:

```text
External table URI khong match file upload.
```

Cach debug:

```text
So sanh log upload `gs://...parquet` voi DDL `uris = ['gs://.../*.parquet']`.
```

Loi cot sai kieu:

```text
Parquet schema khong on dinh giua cac ngay.
```

Cach sua:

```text
Luon tao day du expected columns va ep kieu truoc khi ghi Parquet.
```

