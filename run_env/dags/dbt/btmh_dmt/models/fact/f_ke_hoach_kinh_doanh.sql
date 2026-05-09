{% set xlsx_schema = (var('xlsx_schema', 'stg_ggs') | lower) %}
{% set xlsx_table = var('xlsx_ke_hoach_table', 'xlsx_ke_hoach_kinh_doanh') %}

with df_xlsx as (
  select
    'XLSX' as Nguon,
    cast(Ngay as date) as Ngay,
    cast(Ma_CH as string) as Ma_CH,
    cast(Nhom_SP_Lon as string) as Nhom_SP_Lon,
    cast(Nhom_SP_Nho as string) as Nhom_SP_Nho,
    safe_cast(San_luong_KH_ngay as float64) as San_luong_KH_ngay,
    safe_cast(Doanh_thu_KH_ngay as float64) as Doanh_thu_KH_ngay,
    safe_cast(Loi_nhuan_KH_ngay as float64) as Loi_nhuan_KH_ngay
  from {{ source('ggs', xlsx_table) }}
),

df_ggs as (
  select
    'GGS' as Nguon,
    cast(Ngay as date) as Ngay,
    cast(Ma_CH as string) as Ma_CH,
    cast(Nhom_SP_Lon as string) as Nhom_SP_Lon,
    cast(Nhom_SP_Nho as string) as Nhom_SP_Nho,
    safe_cast(San_luong_KH_ngay as float64) as San_luong_KH_ngay,
    safe_cast(Doanh_thu_KH_ngay as float64) as Doanh_thu_KH_ngay,
    safe_cast(Loi_nhuan_KH_ngay as float64) as Loi_nhuan_KH_ngay
  from {{ ref('f_ggs_ke_hoach_kinh_doanh') }}
  where cast(Ngay as date) > date('2025-05-31')
),

unioned as (
  select * from df_xlsx
  union all
  select * from df_ggs
),

final as (
  select
    * except (Nguon)
  from unioned
  qualify row_number() over (
    partition by Ngay, Ma_CH, Nhom_SP_Lon, Nhom_SP_Nho
    order by Nguon asc
  ) = 1
)

select *
from final
