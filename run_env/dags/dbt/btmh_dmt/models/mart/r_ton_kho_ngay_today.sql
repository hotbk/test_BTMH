{{
  config(
    materialized='table',
    on_schema_change='sync_all_columns'
  )
}}

select *
from {{ ref('r_ton_kho_ngay') }}
-- Use the same run date as the pipeline for consistency and backfill support.
where Ngay = {{ btmh_run_date() }}
