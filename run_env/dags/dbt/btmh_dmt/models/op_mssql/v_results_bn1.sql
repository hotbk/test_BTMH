{{ config(tags=['mssql_ingestion']) }}

select
    cast(column1 as string) as column1,
    safe_cast(column2 as numeric) as column2,
    _btmh_ingested_at,
    _btmh_source_table
from {{ source('mssql', 'results_bn1') }}
