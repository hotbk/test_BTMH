{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    partition_by={"field": "ngay_ky_hd", "data_type": "date"},
    cluster_by=['ma_ncc', 'ma_hd']
  )
}}

with params as (
  select
    {{ btmh_run_date() }} as run_date,
    date_sub({{ btmh_run_date() }}, interval 60 day) as cutoff_date
),

src as (
  select
    *
  from {{ source('ggs', 'ggs_qlhd') }}
  qualify dt = max(dt) over()
),

raw_typed as (
  select
    cast(ma_ncc as string) as ma_ncc,
    cast(ma_hd as string) as ma_hd,

    cast(ngay_ky_hd as string) as ngay_ky_hd_s,
    safe_cast(cast(ngay_ky_hd as string) as int64) as ngay_ky_hd_n,

    cast(id_dai_chi as string) as id_dai_chi,
    cast(ten_dai_chi as string) as ten_dai_chi,
    cast(dong_san_pham as string) as dong_san_pham,

    cast(ngay_giao_du_kien as string) as ngay_giao_du_kien_s,
    safe_cast(cast(ngay_giao_du_kien as string) as int64) as ngay_giao_du_kien_n,

    safe_cast(sl_dat_theo_hd as int64) as sl_dat_theo_hd,
    safe_cast(sl_sap_giao as int64) as sl_sap_giao,
    safe_cast(tong_giao as int64) as tong_giao,

    cast(ngay_nhan_hang_l1 as string) as ngay_nhan_hang_l1_s,
    safe_cast(cast(ngay_nhan_hang_l1 as string) as int64) as ngay_nhan_hang_l1_n,
    safe_cast(sl_nhan_l1 as int64) as sl_nhan_l1,
    safe_cast(sl_loai_l1 as int64) as sl_loai_l1,
    safe_cast(sl_dat_l1 as int64) as sl_dat_l1,

    cast(ngay_nhan_hang_l2 as string) as ngay_nhan_hang_l2_s,
    safe_cast(cast(ngay_nhan_hang_l2 as string) as int64) as ngay_nhan_hang_l2_n,
    safe_cast(sl_nhan_l2 as int64) as sl_nhan_l2,
    safe_cast(sl_loai_l2 as int64) as sl_loai_l2,
    safe_cast(sl_dat_l2 as int64) as sl_dat_l2,

    cast(ngay_nhan_hang_l3 as string) as ngay_nhan_hang_l3_s,
    safe_cast(cast(ngay_nhan_hang_l3 as string) as int64) as ngay_nhan_hang_l3_n,
    safe_cast(sl_nhan_l3 as int64) as sl_nhan_l3,
    safe_cast(sl_loai_l3 as int64) as sl_loai_l3,
    safe_cast(sl_dat_l3 as int64) as sl_dat_l3,

    cast(ngay_nhan_hang_l4 as string) as ngay_nhan_hang_l4_s,
    safe_cast(cast(ngay_nhan_hang_l4 as string) as int64) as ngay_nhan_hang_l4_n,
    safe_cast(sl_nhan_l4 as int64) as sl_nhan_l4,
    safe_cast(sl_loai_l4 as int64) as sl_loai_l4,
    safe_cast(sl_dat_l4 as int64) as sl_dat_l4,

    current_timestamp() as UpdateTime,

    p.cutoff_date as cutoff_date

  from src
  cross join params p
),

typed as (
  select
    ma_ncc,
    ma_hd,

    -- Convert epoch seconds/millis/micros/nanos (or string timestamp) to DATE.
    date(
      coalesce(
        safe_cast(ngay_ky_hd_s as timestamp),
        case
          when ngay_ky_hd_n is null then null
          when abs(ngay_ky_hd_n) < 100000000000 then timestamp_seconds(ngay_ky_hd_n)
          when abs(ngay_ky_hd_n) < 100000000000000 then timestamp_millis(ngay_ky_hd_n)
          when abs(ngay_ky_hd_n) < 100000000000000000 then timestamp_micros(ngay_ky_hd_n)
          else timestamp_micros(div(ngay_ky_hd_n, 1000))
        end
      )
    ) as ngay_ky_hd,

    id_dai_chi,
    ten_dai_chi,
    dong_san_pham,

    date(
      coalesce(
        safe_cast(ngay_giao_du_kien_s as timestamp),
        case
          when ngay_giao_du_kien_n is null then null
          when abs(ngay_giao_du_kien_n) < 100000000000 then timestamp_seconds(ngay_giao_du_kien_n)
          when abs(ngay_giao_du_kien_n) < 100000000000000 then timestamp_millis(ngay_giao_du_kien_n)
          when abs(ngay_giao_du_kien_n) < 100000000000000000 then timestamp_micros(ngay_giao_du_kien_n)
          else timestamp_micros(div(ngay_giao_du_kien_n, 1000))
        end
      )
    ) as ngay_giao_du_kien,

    sl_dat_theo_hd,
    sl_sap_giao,
    tong_giao,

    date(
      coalesce(
        safe_cast(ngay_nhan_hang_l1_s as timestamp),
        case
          when ngay_nhan_hang_l1_n is null then null
          when abs(ngay_nhan_hang_l1_n) < 100000000000 then timestamp_seconds(ngay_nhan_hang_l1_n)
          when abs(ngay_nhan_hang_l1_n) < 100000000000000 then timestamp_millis(ngay_nhan_hang_l1_n)
          when abs(ngay_nhan_hang_l1_n) < 100000000000000000 then timestamp_micros(ngay_nhan_hang_l1_n)
          else timestamp_micros(div(ngay_nhan_hang_l1_n, 1000))
        end
      )
    ) as ngay_nhan_hang_l1,
    sl_nhan_l1,
    sl_loai_l1,
    sl_dat_l1,

    date(
      coalesce(
        safe_cast(ngay_nhan_hang_l2_s as timestamp),
        case
          when ngay_nhan_hang_l2_n is null then null
          when abs(ngay_nhan_hang_l2_n) < 100000000000 then timestamp_seconds(ngay_nhan_hang_l2_n)
          when abs(ngay_nhan_hang_l2_n) < 100000000000000 then timestamp_millis(ngay_nhan_hang_l2_n)
          when abs(ngay_nhan_hang_l2_n) < 100000000000000000 then timestamp_micros(ngay_nhan_hang_l2_n)
          else timestamp_micros(div(ngay_nhan_hang_l2_n, 1000))
        end
      )
    ) as ngay_nhan_hang_l2,
    sl_nhan_l2,
    sl_loai_l2,
    sl_dat_l2,

    date(
      coalesce(
        safe_cast(ngay_nhan_hang_l3_s as timestamp),
        case
          when ngay_nhan_hang_l3_n is null then null
          when abs(ngay_nhan_hang_l3_n) < 100000000000 then timestamp_seconds(ngay_nhan_hang_l3_n)
          when abs(ngay_nhan_hang_l3_n) < 100000000000000 then timestamp_millis(ngay_nhan_hang_l3_n)
          when abs(ngay_nhan_hang_l3_n) < 100000000000000000 then timestamp_micros(ngay_nhan_hang_l3_n)
          else timestamp_micros(div(ngay_nhan_hang_l3_n, 1000))
        end
      )
    ) as ngay_nhan_hang_l3,
    sl_nhan_l3,
    sl_loai_l3,
    sl_dat_l3,

    date(
      coalesce(
        safe_cast(ngay_nhan_hang_l4_s as timestamp),
        case
          when ngay_nhan_hang_l4_n is null then null
          when abs(ngay_nhan_hang_l4_n) < 100000000000 then timestamp_seconds(ngay_nhan_hang_l4_n)
          when abs(ngay_nhan_hang_l4_n) < 100000000000000 then timestamp_millis(ngay_nhan_hang_l4_n)
          when abs(ngay_nhan_hang_l4_n) < 100000000000000000 then timestamp_micros(ngay_nhan_hang_l4_n)
          else timestamp_micros(div(ngay_nhan_hang_l4_n, 1000))
        end
      )
    ) as ngay_nhan_hang_l4,
    sl_nhan_l4,
    sl_loai_l4,
    sl_dat_l4,

    UpdateTime,
    cutoff_date

  from raw_typed
),

filtered as (
  select
    *
  from typed
  where ma_ncc is not null
    and ma_hd is not null
    and ngay_ky_hd >= cutoff_date
)

select *
from filtered
where ngay_ky_hd is not null
