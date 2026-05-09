{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ngay', 'ma_ch', 'nhom_sp_lon', 'nhom_sp_nho'],
    on_schema_change='sync_all_columns',
    partition_by={"field": "ngay", "data_type": "date", "granularity": "month"},
    cluster_by=['ma_ch', 'nhom_sp_lon', 'nhom_sp_nho'],
  )
}}

with src as (
  select
    *
  from {{ source('ggs', 'ggs_ke_hoach') }}
  qualify dt = max(dt) over()
),

prep as (
  select
    safe_cast(nam as int64) as nam,
    safe_cast(thang as int64) as thang,
    cast(ma_ch as string) as ma_ch,
    cast(nhom_sp_lon as string) as nhom_sp_lon,
    cast(nhom_sp_nho as string) as nhom_sp_nho,

    -- Monthly totals (use coalesce to be tolerant of upstream naming)
    safe_cast(coalesce(san_luong_kh_thang, so_luong) as float64) as san_luong_kh_thang,
    safe_cast(coalesce(doanh_thu_thang, doanh_thu) as float64) as doanh_thu_thang,
    safe_cast(coalesce(loi_nhuan_thang, lai_gop) as float64) as loi_nhuan_thang,

    -- Day columns (numeric)
    safe_cast(ngay_1 as float64) as ngay_1,
    safe_cast(ngay_2 as float64) as ngay_2,
    safe_cast(ngay_3 as float64) as ngay_3,
    safe_cast(ngay_4 as float64) as ngay_4,
    safe_cast(ngay_5 as float64) as ngay_5,
    safe_cast(ngay_6 as float64) as ngay_6,
    safe_cast(ngay_7 as float64) as ngay_7,
    safe_cast(ngay_8 as float64) as ngay_8,
    safe_cast(ngay_9 as float64) as ngay_9,
    safe_cast(ngay_10 as float64) as ngay_10,
    safe_cast(ngay_11 as float64) as ngay_11,
    safe_cast(ngay_12 as float64) as ngay_12,
    safe_cast(ngay_13 as float64) as ngay_13,
    safe_cast(ngay_14 as float64) as ngay_14,
    safe_cast(ngay_15 as float64) as ngay_15,
    safe_cast(ngay_16 as float64) as ngay_16,
    safe_cast(ngay_17 as float64) as ngay_17,
    safe_cast(ngay_18 as float64) as ngay_18,
    safe_cast(ngay_19 as float64) as ngay_19,
    safe_cast(ngay_20 as float64) as ngay_20,
    safe_cast(ngay_21 as float64) as ngay_21,
    safe_cast(ngay_22 as float64) as ngay_22,
    safe_cast(ngay_23 as float64) as ngay_23,
    safe_cast(ngay_24 as float64) as ngay_24,
    safe_cast(ngay_25 as float64) as ngay_25,
    safe_cast(ngay_26 as float64) as ngay_26,
    safe_cast(ngay_27 as float64) as ngay_27,
    safe_cast(ngay_28 as float64) as ngay_28,
    safe_cast(ngay_29 as float64) as ngay_29,
    safe_cast(ngay_30 as float64) as ngay_30,
    safe_cast(ngay_31 as float64) as ngay_31
  from src
),

unpivoted as (
  select
    p.nam,
    p.thang,
    p.ma_ch,
    p.nhom_sp_lon,
    p.nhom_sp_nho,
    p.san_luong_kh_thang,
    p.doanh_thu_thang,
    p.loi_nhuan_thang,
    x.ngay_str,
    x.gia_tri
  from prep p
  cross join unnest([
    struct('Ngay_1' as ngay_str, p.ngay_1 as gia_tri),
    struct('Ngay_2' as ngay_str, p.ngay_2 as gia_tri),
    struct('Ngay_3' as ngay_str, p.ngay_3 as gia_tri),
    struct('Ngay_4' as ngay_str, p.ngay_4 as gia_tri),
    struct('Ngay_5' as ngay_str, p.ngay_5 as gia_tri),
    struct('Ngay_6' as ngay_str, p.ngay_6 as gia_tri),
    struct('Ngay_7' as ngay_str, p.ngay_7 as gia_tri),
    struct('Ngay_8' as ngay_str, p.ngay_8 as gia_tri),
    struct('Ngay_9' as ngay_str, p.ngay_9 as gia_tri),
    struct('Ngay_10' as ngay_str, p.ngay_10 as gia_tri),
    struct('Ngay_11' as ngay_str, p.ngay_11 as gia_tri),
    struct('Ngay_12' as ngay_str, p.ngay_12 as gia_tri),
    struct('Ngay_13' as ngay_str, p.ngay_13 as gia_tri),
    struct('Ngay_14' as ngay_str, p.ngay_14 as gia_tri),
    struct('Ngay_15' as ngay_str, p.ngay_15 as gia_tri),
    struct('Ngay_16' as ngay_str, p.ngay_16 as gia_tri),
    struct('Ngay_17' as ngay_str, p.ngay_17 as gia_tri),
    struct('Ngay_18' as ngay_str, p.ngay_18 as gia_tri),
    struct('Ngay_19' as ngay_str, p.ngay_19 as gia_tri),
    struct('Ngay_20' as ngay_str, p.ngay_20 as gia_tri),
    struct('Ngay_21' as ngay_str, p.ngay_21 as gia_tri),
    struct('Ngay_22' as ngay_str, p.ngay_22 as gia_tri),
    struct('Ngay_23' as ngay_str, p.ngay_23 as gia_tri),
    struct('Ngay_24' as ngay_str, p.ngay_24 as gia_tri),
    struct('Ngay_25' as ngay_str, p.ngay_25 as gia_tri),
    struct('Ngay_26' as ngay_str, p.ngay_26 as gia_tri),
    struct('Ngay_27' as ngay_str, p.ngay_27 as gia_tri),
    struct('Ngay_28' as ngay_str, p.ngay_28 as gia_tri),
    struct('Ngay_29' as ngay_str, p.ngay_29 as gia_tri),
    struct('Ngay_30' as ngay_str, p.ngay_30 as gia_tri),
    struct('Ngay_31' as ngay_str, p.ngay_31 as gia_tri)
  ]) x
  where x.gia_tri is not null
),

final as (
  select
    safe.parse_date(
      '%Y-%m-%d',
      format('%04d-%02d-%02d', nam, thang, cast(regexp_replace(ngay_str, 'Ngay_', '') as int64))
    ) as ngay,

    case
      when upper(ma_ch) = 'BN' then 'BN1'
      when upper(ma_ch) = 'HD' then 'HD1'
      else ma_ch
    end as ma_ch,

    nhom_sp_lon,
    nhom_sp_nho,

    gia_tri as san_luong_kh_ngay,

    (doanh_thu_thang * gia_tri) / nullif(san_luong_kh_thang, 0) as doanh_thu_kh_ngay,
    (loi_nhuan_thang * gia_tri) / nullif(san_luong_kh_thang, 0) as loi_nhuan_kh_ngay
  from unpivoted
)

select *
from final
where ngay is not null
