{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    partition_by={"field": "Ngay", "data_type": "date"},
    cluster_by=['Ma_VBTG']
  )
}}

{% set src_schema = (var('vbtg_schema', 'stg_augges_225') | lower) %}

with params as (
  select
    {{ btmh_run_date() }} as run_date,
    {{ btmh_start_date(0) }} as _unused_start_date
),

valid as (
  select distinct cast(Ma_VBTG as string) as Ma_VBTG
  from {{ source(src_schema, 'dmvbtg') }}
),

src as (
  select
    cast(a.Ma_VBTG as string) as Ma_VBTG,
    cast(a.Ten_VBTG as string) as Ten_VBTG,
    safe_cast(a.TyGia_Mua as bignumeric) as TyGia_Mua,
    safe_cast(a.TyGia_Ban as bignumeric) as TyGia_Ban,
    safe_cast(a.LastEdit as timestamp) as LastEdit
  from {{ source(src_schema, 'dmvbtg_a') }} a
),

clean as (
  select s.*
  from src s
  join valid v
    on s.Ma_VBTG = v.Ma_VBTG
  where s.TyGia_Ban is not null
    and s.TyGia_Mua is not null
    and s.TyGia_Ban <= 50000000
    and s.TyGia_Mua <= 50000000
    and s.LastEdit is not null
),

-- (giá mở cửa / hoặc giá đóng cửa ngày trước)
daily_data as (
  select
    d.Ma_VBTG,
    d.Ten_VBTG,
    d.TyGia_Mua,
    d.TyGia_Ban,
    d.LastEdit
  from (
    select
      c.*,
      row_number() over (
        partition by c.Ma_VBTG
        order by c.LastEdit desc
      ) as rn
    from clean c
    cross join params p
    where c.LastEdit <= timestamp(p.run_date)
  ) d
  where d.rn = 1
),

intraday as (
  select c.*
  from clean c
  cross join params p
  where date(c.LastEdit) = p.run_date
),

max_price as (
  select
    x.Ma_VBTG,
    x.TyGia_Ban as TyGia_Ban_Max,
    x.TyGia_Mua as TyGia_Mua_At_Max
  from (
    select
      i.*,
      row_number() over (
        partition by i.Ma_VBTG
        order by i.TyGia_Ban desc, i.LastEdit desc
      ) as rn
    from intraday i
  ) x
  where x.rn = 1
),

-- Min/Avg chỉ áp dụng lọc kỹ cho KGB và TyGia_Ban >= 1,000,000
min_price as (
  select
    x.Ma_VBTG,
    x.TyGia_Ban as TyGia_Ban_Min,
    x.TyGia_Mua as TyGia_Mua_At_Min
  from (
    select
      i.*,
      row_number() over (
        partition by i.Ma_VBTG
        order by i.TyGia_Ban asc, i.LastEdit desc
      ) as rn
    from intraday i
    where i.Ma_VBTG = 'KGB'
      and i.TyGia_Ban >= 1000000
  ) x
  where x.rn = 1
),

avg_price as (
  select
    i.Ma_VBTG,
    avg(i.TyGia_Ban) as TyGia_Ban_Avg,
    avg(i.TyGia_Mua) as TyGia_Mua_Avg
  from intraday i
  where i.Ma_VBTG = 'KGB'
    and i.TyGia_Ban >= 1000000
  group by i.Ma_VBTG
),

final as (
  select distinct
    p.run_date as Ngay,
    d.Ma_VBTG as Ma_VBTG,
    d.Ten_VBTG as Ten_VBTG,

    coalesce(mp.TyGia_Ban_Max, d.TyGia_Ban) as Gia_Ban_Cao_Nhat,
    coalesce(mn.TyGia_Ban_Min, d.TyGia_Ban) as Gia_Ban_Thap_Nhat,

    coalesce(mp.TyGia_Mua_At_Max, d.TyGia_Mua) as Gia_Mua_Theo_Gia_Ban_Cao_Nhat,
    coalesce(mn.TyGia_Mua_At_Min, d.TyGia_Mua) as Gia_Mua_Theo_Gia_Ban_Thap_Nhat,

    coalesce(ap.TyGia_Ban_Avg, d.TyGia_Ban) as Gia_Ban_Trung_Binh,
    coalesce(ap.TyGia_Mua_Avg, d.TyGia_Mua) as Gia_Mua_Trung_Binh,

    current_timestamp() as UpdateTime

  from daily_data d
  cross join params p
  left join max_price mp
    on d.Ma_VBTG = mp.Ma_VBTG
  left join min_price mn
    on d.Ma_VBTG = mn.Ma_VBTG
  left join avg_price ap
    on d.Ma_VBTG = ap.Ma_VBTG
)

select *
from final

{% if is_incremental() %}
where Ngay = {{ btmh_run_date() }}
{% endif %}
