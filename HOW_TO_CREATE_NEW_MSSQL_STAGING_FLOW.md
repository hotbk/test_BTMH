# Huong Dan Tao Luong MSSQL -> BigQuery Staging -> dbt Moi

Tai lieu nay huong dan tung buoc de tao mot luong moi giong luong mau dang co:

```text
MSSQL table
    -> Airflow load len BigQuery staging
    -> dbt source
    -> dbt model/view
```

Luong mau hien tai dang dung bang:

```text
vietdb.dbo.Results_BN1
```

va load len:

```text
btmh_stg_mssql.results_bn1
```

## 1. Chon bang MSSQL can keo

Truoc tien can xac dinh 4 thong tin:

```text
source_database
source_schema
source_table
destination_table
```

Vi du:

```text
source_database: vietdb
source_schema: dbo
source_table: Results_BN1
destination_table: results_bn1
```

Tuong ung SQL source:

```sql
select *
from [vietdb].[dbo].[Results_BN1]
```

Tuong ung BigQuery staging table:

```text
btmh_stg_mssql.results_bn1
```

## 2. Kiem tra Airflow connection MSSQL

DAG dang dung connection:

```text
src_mssql
```

Kiem tra trong Airflow UI:

```text
Admin -> Connections -> src_mssql
```

Voi `MsSqlHook`, nen de Extra trong connection trong hoac chi chua tham so ma `pymssql` ho tro.

Khong nen de cac key ODBC sau trong Extra:

```text
driver
trustServerCertificate
TrustServerCertificate
Encrypt
encrypt
```

Neu can test connection, co the chay DAG:

```text
test_mssql_connection
```

## 3. Them bang vao Airflow Variable

DAG load staging hien tai la:

```text
run_env/dags/mssql_to_bigquery_dbt.py
```

DAG doc danh sach bang tu Airflow Variable:

```text
mssql_to_bq_tables
```

Vao Airflow UI:

```text
Admin -> Variables -> + Add
```

Tao variable:

```text
Key: mssql_to_bq_tables
```

Gia tri mau cho 1 bang:

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

Neu muon keo nhieu bang, them object vao list:

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
  },
  {
    "source_database": "vietdb",
    "source_schema": "dbo",
    "source_table": "Results_MH",
    "destination_dataset": "btmh_stg_mssql",
    "destination_table": "results_mh",
    "write_disposition": "WRITE_TRUNCATE",
    "batch_size": 50000
  }
]
```

Giai thich cac field:

```text
source_database     Database tren MSSQL
source_schema       Schema tren MSSQL, thuong la dbo
source_table        Ten bang tren MSSQL
destination_dataset Dataset BigQuery staging
destination_table   Ten bang tren BigQuery staging
write_disposition   Cach ghi vao BigQuery
batch_size          So dong moi lan doc tu MSSQL
```

Voi staging full-refresh, dung:

```text
WRITE_TRUNCATE
```

Nghia la moi lan chay se ghi de staging table bang du lieu moi.

## 4. Khai bao dbt source

Mo file:

```text
run_env/dags/dbt/btmh_dmt/models/sources.yml
```

Tim source:

```yaml
- name: mssql
  schema: "{{ var('mssql_schema', env_var('MSSQL_BQ_DATASET', 'btmh_stg_mssql')) }}"
  tables:
    - name: results_bn1
```

Neu them bang moi, them ten table vao `tables`.

Vi du them `results_mh`:

```yaml
- name: mssql
  schema: "{{ var('mssql_schema', env_var('MSSQL_BQ_DATASET', 'btmh_stg_mssql')) }}"
  tables:
    - name: results_bn1
    - name: results_mh
```

Sau khi khai bao, dbt co the doc bang staging bang cu phap:

```sql
{{ source('mssql', 'results_mh') }}
```

## 5. Tao dbt model moi

Cac model MSSQL nam trong folder:

```text
run_env/dags/dbt/btmh_dmt/models/op_mssql/
```

Quy uoc dat ten file:

```text
v_<ten_bang>.sql
```

Vi du:

```text
run_env/dags/dbt/btmh_dmt/models/op_mssql/v_results_mh.sql
```

Noi dung mau:

```sql
{{ config(tags=['mssql_ingestion']) }}

select
    *
from {{ source('mssql', 'results_mh') }}
```

Nen ep kieu cot ro rang neu biet schema.

Vi du:

```sql
{{ config(tags=['mssql_ingestion']) }}

select
    cast(column1 as string) as column1,
    safe_cast(column2 as numeric) as column2,
    _btmh_ingested_at,
    _btmh_source_table
from {{ source('mssql', 'results_mh') }}
```

Luu y tag:

```text
mssql_ingestion
```

DAG chi chay cac dbt model co tag nay.

## 6. Kiem tra dbt_project.yml

File:

```text
run_env/dags/dbt/btmh_dmt/dbt_project.yml
```

Can co cau hinh:

```yaml
op_mssql:
  +schema: mart
  +materialized: view
```

Neu da co thi khong can them lai.

Cau hinh nay nghia la:

```text
models/op_mssql/*.sql
```

se duoc tao thanh view trong dataset mart theo convention cua project.

## 7. Chay DAG

Vao Airflow UI va trigger DAG:

```text
mssql_to_bigquery_dbt
```

DAG se chay 2 task:

```text
load_mssql_tables_to_bigquery
run_dbt_mssql_models
```

Task 1:

```text
load_mssql_tables_to_bigquery
```

Lam nhiem vu:

```text
MSSQL -> BigQuery staging
```

Task 2:

```text
run_dbt_mssql_models
```

Lam nhiem vu:

```text
BigQuery staging -> dbt model/view
```

## 8. Kiem tra ket qua tren BigQuery

Sau khi DAG thanh cong, kiem tra bang staging:

```text
btmh_stg_mssql.<destination_table>
```

Vi du:

```text
btmh_stg_mssql.results_bn1
```

Sau do kiem tra view do dbt tao trong dataset mart.

Ten dataset mart phu thuoc vao cau hinh `dbt_project.yml` va macro `generate_schema_name.sql`.

Voi config hien tai, cac model `op_mssql` se vao schema:

```text
mart
```

theo fixed prefix cua project.

## 9. Checklist khi tao luong moi

Truoc khi bao hoan thanh, kiem tra:

```text
[ ] Bang MSSQL ton tai va query select * chay duoc.
[ ] Airflow connection src_mssql ket noi duoc.
[ ] Airflow Variable mssql_to_bq_tables co khai bao bang moi.
[ ] sources.yml da co table moi trong source mssql.
[ ] Da tao model moi trong models/op_mssql/.
[ ] Model co tag mssql_ingestion.
[ ] DAG mssql_to_bigquery_dbt chay thanh cong.
[ ] BigQuery staging table co du lieu.
[ ] dbt view/model duoc tao thanh cong.
```

## 10. Loi thuong gap

### Loi connection MSSQL

Neu gap loi connect, kiem tra `src_mssql`.

Loi thuong gap:

```text
connect() got an unexpected keyword argument 'driver'
connect() got an unexpected keyword argument 'trustServerCertificate'
```

Nguyen nhan:

```text
Extra cua connection dang co tham so ODBC, nhung DAG dung MsSqlHook/pymssql.
```

Cach xu ly:

```text
Xoa cac key ODBC khoi Extra.
```

### Loi BigQuery permission

Neu task load fail do permission, kiem tra service account GCP.

Can quyen:

```text
bigquery.datasets.create
bigquery.tables.create
bigquery.tables.updateData
bigquery.jobs.create
```

### Loi dbt source not found

Kiem tra `sources.yml` da co table moi chua.

### Loi dbt column not found

Kiem tra model `.sql` co dung ten cot trong BigQuery staging table khong.

### Loi dataset/table khong dung ten

Kiem tra Airflow Variable:

```text
mssql_to_bq_tables
```

Dac biet cac field:

```text
destination_dataset
destination_table
```

