{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['Nguon', 'Nam', 'Mm', 'Ma_Kho', 'ID_Hang'],
    on_schema_change='sync_all_columns',
    pre_hook=[
      "{% if is_incremental() %}"
      "delete from {{ this }} "
      "where Nam = extract(year from {{ btmh_run_date() }}) "
      "  and Mm = extract(month from {{ btmh_run_date() }});"
      "{% endif %}"
    ]
  )
}}

{% set sources = var('htk_sources') %}

with params as (
  select
    extract(year from {{ btmh_run_date() }}) as run_year,
    extract(month from {{ btmh_run_date() }}) as run_month
),

dmh as (
  select *
  from {{ ref('d_hang') }}
),

kho_dim as (
  select *
  from {{ ref('d_kho') }}
),


{% for s in sources %}
src_{{ s.code | lower }} as (
  select
    '{{ s.code }}' as Nguon,
    cast(htk.Nam as int64) as Nam,
    cast(htk.Mm as int64) as Mm,

    {% if s.code in ['HD', 'BN'] %}
    case
      {% if s.code == 'HD' %}
      when cast(kho.Ma_Kho as string) = 'CH1' then 'HD1'
      when cast(kho.Ma_Kho as string) = 'KM01' then 'KMHD1'
      {% elif s.code == 'BN' %}
      when cast(kho.Ma_Kho as string) = 'CH1' then 'BN1'
      when cast(kho.Ma_Kho as string) = 'KM01' then 'KMBN1'
      {% endif %}
      else cast(kho.Ma_Kho as string)
    end as Ma_Kho,
    {% else %}
    cast(kho.Ma_Kho as string) as Ma_Kho,
    {% endif %}

    cast(htk.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as float64), 0)
      + coalesce(safe_cast(h.The_Tich as float64), 0)
      + coalesce(safe_cast(h.Tien_Lai as float64), 0)
      + coalesce(safe_cast(h.Tyle_Lai as float64), 0)
    ) as Tong_tlg,

    safe_cast(htk.Don_Gia1 as float64) as Don_Gia1,
    safe_cast(htk.T_Tien1 as float64) as T_Tien1,
    safe_cast(htk.So_Luong as float64) as So_Luong,
    safe_cast(htk.Sl_Qd as float64) as Sl_Qd,
    safe_cast(htk.Gia_NM as float64) as Gia_NM,

    safe.parse_date('%Y%m%d', concat('20', cast(htk.Sngay as string))) as Ngay_Nhap,

    current_timestamp() as UpdateTime
  from {{ source(btmh_source_name(s), 'htk') }} htk
  left join {{ source(btmh_source_name(s), 'dmkho') }} kho_raw
    on safe_cast(htk.ID_Kho as int64) = safe_cast(kho_raw.ID as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)
  left join dmh h
    on safe_cast(htk.ID_Hang as int64) = safe_cast(h.ID as int64)
  cross join params p
  where safe_cast(htk.Nam as int64) = p.run_year
    and safe_cast(htk.Mm as int64) = p.run_month
)
{% if not loop.last %},{% endif %}
{% endfor %}

select * from (
{% for s in sources %}
  select * from src_{{ s.code | lower }}
  {% if not loop.last %}union all{% endif %}
{% endfor %}
)
