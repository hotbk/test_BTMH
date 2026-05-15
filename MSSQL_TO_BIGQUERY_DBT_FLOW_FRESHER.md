# Luong MSSQL -> BigQuery -> dbt Cho Fresher

Tai lieu nay giai thich luong moi duoc them vao project de keo du lieu tu MSSQL len BigQuery, sau do dung dbt de transform.

## 1. Muc tieu

Luong nay lam 3 viec:

1. Airflow ket noi vao MSSQL.
2. Airflow copy bang `vietdb.dbo.Results_BN1` sang BigQuery.
3. Airflow goi dbt de tao view/model tu bang raw tren BigQuery.

Duong di du lieu:

```text
MSSQL: vietdb.dbo.Results_BN1
        |
        v
Airflow DAG: mssql_to_bigquery_dbt
        |
        v
BigQuery raw table: btmh_stg_mssql.results_bn1
        |
        v
dbt model: op_mssql/v_results_bn1.sql
        |
        v
BigQuery mart view
```

## 2. Cac file lien quan

### Airflow DAG

File:

```text
run_env/dags/mssql_to_bigquery_dbt.py
```

DAG name:

```text
mssql_to_bigquery_dbt
```

DAG nay co 2 task:

```text
load_mssql_tables_to_bigquery
        |
        v
run_dbt_mssql_models
```

### dbt source

File:

```text
run_env/dags/dbt/btmh_dmt/models/sources.yml
```

Source moi:

```yaml
- name: mssql
  schema: "{{ var('mssql_schema', env_var('MSSQL_BQ_DATASET', 'btmh_stg_mssql')) }}"
  tables:
    - name: results_bn1
```

Source nay giup dbt hieu rang:

```sql
{{ source('mssql', 'results_bn1') }}
```

tro toi bang BigQuery:

```text
btmh_stg_mssql.results_bn1
```

### dbt model

File:

```text
run_env/dags/dbt/btmh_dmt/models/op_mssql/v_results_bn1.sql
```

Model nay doc tu raw table va tao view sau transform.

### dbt project config

File:

```text
run_env/dags/dbt/btmh_dmt/dbt_project.yml
```

Folder `op_mssql` duoc cau hinh:

```yaml
op_mssql:
  +schema: mart
  +materialized: view
```

Nghia la cac model trong folder `op_mssql` se duoc tao thanh view trong dataset mart theo convention cua project.

## 3. Task 1: Keo du lieu tu MSSQL sang BigQuery

Task:

```text
load_mssql_tables_to_bigquery
```

Task nay dung Airflow connection:

```text
src_mssql
```

Connection nay nam trong Airflow UI:

```text
Admin -> Connections -> src_mssql
```

Bang MSSQL dang dung lam mau:

```sql
select *
from [vietdb].[dbo].[Results_BN1]
```

Du lieu duoc load sang BigQuery:

```text
btmh_stg_mssql.results_bn1
```

Che do ghi:

```text
WRITE_TRUNCATE
```

Nghia la moi lan DAG chay, bang `results_bn1` tren BigQuery se bi ghi de bang du lieu moi tu MSSQL.

Ngoai cac cot goc tu MSSQL, DAG them 2 cot metadata:

```text
_btmh_ingested_at
_btmh_source_table
```

Y nghia:

- `_btmh_ingested_at`: thoi diem Airflow keo du lieu.
- `_btmh_source_table`: ten bang nguon MSSQL.

## 4. Task 2: Chay dbt model

Task:

```text
run_dbt_mssql_models
```

Task nay chay lenh:

```bash
cd /opt/airflow/dags/dbt/btmh_dmt && /home/airflow/.local/bin/dbt run --profiles-dir . --select tag:mssql_ingestion
```

Lenh nay chi chay cac model co tag:

```text
mssql_ingestion
```

Model hien tai:

```text
run_env/dags/dbt/btmh_dmt/models/op_mssql/v_results_bn1.sql
```

No co config:

```sql
{{ config(tags=['mssql_ingestion']) }}
```

## 5. Noi dung model dbt

Model `v_results_bn1.sql`:

```sql
{{ config(tags=['mssql_ingestion']) }}

select
    cast(column1 as string) as column1,
    safe_cast(column2 as numeric) as column2,
    _btmh_ingested_at,
    _btmh_source_table
from {{ source('mssql', 'results_bn1') }}
```

Giai thich:

- `column1` duoc ep kieu thanh `string`.
- `column2` duoc ep kieu thanh `numeric`.
- Giu lai 2 cot metadata de biet du lieu duoc lay tu dau va lay luc nao.

## 6. Cach doi bang can keo

DAG co the doc cau hinh tu Airflow Variable:

```text
mssql_to_bq_tables
```

Gia tri mau:

```json
[
  {
    "source_database": "vietdb",
    "source_schema": "dbo",
    "source_table": "Results_BN1",
    "destination_dataset": "btmh_stg_mssql",
    "destination_table": "results_bn1",
    "write_disposition": "WRITE_TRUNCATE",
    "batch_size": 50000
  }
]
```

Neu khong tao Variable nay, DAG se dung cau hinh mac dinh trong code.

## 7. Cach chay

1. Dam bao Airflow connection `src_mssql` da ket noi duoc MSSQL.
2. Dam bao GCP service account da co quyen tao dataset/table va load data vao BigQuery.
3. Vao Airflow UI.
4. Bat DAG:

```text
mssql_to_bigquery_dbt
```

5. Trigger DAG thu cong.
6. Kiem tra task `load_mssql_tables_to_bigquery`.
7. Kiem tra task `run_dbt_mssql_models`.
8. Vao BigQuery kiem tra raw table:

```text
btmh_stg_mssql.results_bn1
```

9. Kiem tra view/model do dbt tao trong mart dataset.

## 8. Loi thuong gap

### Sai Airflow connection

Neu connection `src_mssql` sai, task dau tien se fail.

Can kiem tra:

- Host
- Port
- Database/schema
- Login
- Password
- Extra phai phu hop voi `MsSqlHook`

Voi `MsSqlHook`, khong nen de cac key ODBC nhu:

```text
driver
trustServerCertificate
Encrypt
```

### BigQuery khong co quyen

Neu service account khong co quyen, task load se fail.

Can quyen toi thieu:

- Tao dataset neu dataset chua co.
- Tao table.
- Ghi data vao table.

### dbt khong tim thay source

Neu dbt bao khong tim thay source `mssql.results_bn1`, kiem tra file:

```text
run_env/dags/dbt/btmh_dmt/models/sources.yml
```

### dbt model fail do sai ten cot

Neu bang MSSQL doi ten cot, model:

```text
run_env/dags/dbt/btmh_dmt/models/op_mssql/v_results_bn1.sql
```

phai duoc cap nhat theo ten cot moi.

