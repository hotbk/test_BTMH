{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ma_hang',
    on_schema_change='sync_all_columns'
  )
}}

with src as (
  select *
  from {{ source('ggs', 'ggs_kcs') }}
),

dedup as (
  select
    s.*
  from src s
  qualify row_number() over (
    partition by ma_hang
    order by
      coalesce(
        {{ btmh_to_timestamp_any('ngay_gio_xac_nhan') }},
        {{ btmh_to_timestamp_any('ngay_lam_phieu') }},
        timestamp(datetime(dt), 'Asia/Ho_Chi_Minh')
      ) desc
  ) = 1
)

select *
from dedup
