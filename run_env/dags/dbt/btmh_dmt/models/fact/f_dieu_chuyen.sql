{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    partition_by={"field": "Ngay", "data_type": "date", "granularity": "month"},
    cluster_by=['Nguon', 'Ma_KhoX', 'Ma_KhoN']
  )
}}

{% set sources = var('dieu_chuyen_sources', []) %}
{% if sources | length == 0 %}
  {% do exceptions.raise_compiler_error("Missing dbt var 'dieu_chuyen_sources' (define it in dbt_project.yml)") %}
{% endif %}

with params as (
  select
    {{ btmh_window_end_date() }} as run_date,
    {{ btmh_window_start_date(var('btmh_months_back', 6) | int) }} as start_date,
    date_trunc({{ btmh_window_start_date(var('btmh_months_back', 6) | int) }}, month) as start_month,
    date_add(date_trunc({{ btmh_window_end_date() }}, month), interval 1 month) as end_excl
),

dmh as (
  select *
  from {{ ref('d_hang') }}
),

kho_dim as (
  select *
  from {{ ref('d_kho') }}
),

dt_dim as (
  select *
  from {{ ref('d_doi_tac') }}
)


{% for s in sources %}
, src_{{ s.code | lower }} as (
  select
    cast(d.ID as string) as ID,
    safe_cast(d.Stt as int64) as Stt,
    '{{ s.code }}' as Nguon,

    safe_cast(date(m.Ngay) as date) as Ngay,

    {% if s.fixed_ma_kho_logic == 'HD' %}
      (case
        when cast(khox.Ma_Kho as string) = 'CH1' then 'HD1'
        when cast(khox.Ma_Kho as string) = 'KM01' then 'KMHD1'
        else cast(khox.Ma_Kho as string)
      end) as Ma_KhoX,
      (case
        when cast(khon.Ma_Kho as string) = 'CH1' then 'HD1'
        when cast(khon.Ma_Kho as string) = 'KM01' then 'KMHD1'
        else cast(khon.Ma_Kho as string)
      end) as Ma_KhoN,
      (case
        when cast(khond.Ma_Kho as string) = 'CH1' then 'HD1'
        when cast(khond.Ma_Kho as string) = 'KM01' then 'KMHD1'
        else cast(khond.Ma_Kho as string)
      end) as Ma_KhoND,
    {% elif s.fixed_ma_kho_logic == 'BN' %}
      (case
        when cast(khox.Ma_Kho as string) = 'CH1' then 'BN1'
        when cast(khox.Ma_Kho as string) = 'KM01' then 'KMBN1'
        else cast(khox.Ma_Kho as string)
      end) as Ma_KhoX,
      (case
        when cast(khon.Ma_Kho as string) = 'CH1' then 'BN1'
        when cast(khon.Ma_Kho as string) = 'KM01' then 'KMBN1'
        else cast(khon.Ma_Kho as string)
      end) as Ma_KhoN,
      (case
        when cast(khond.Ma_Kho as string) = 'CH1' then 'BN1'
        when cast(khond.Ma_Kho as string) = 'KM01' then 'KMBN1'
        else cast(khond.Ma_Kho as string)
      end) as Ma_KhoND,
    {% else %}
      cast(khox.Ma_Kho as string) as Ma_KhoX,
      cast(khon.Ma_Kho as string) as Ma_KhoN,
      cast(khond.Ma_Kho as string) as Ma_KhoND,
    {% endif %}

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    cast(
      (
        coalesce(safe_cast(h.T_Luong as bignumeric), 0)
        + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
        + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
        + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
      ) as bignumeric
    ) as Tong_tlg,

    safe_cast(d.So_Luong as bignumeric) as So_luong,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,
    safe_cast(m.Ty_Gia as bignumeric) as Ty_gia,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    cast(d.Ghi_Chu as string) as Ghi_Chu,
    cast(m.Ky_Hieu as string) as Ky_Hieu,
    cast(m.So_Ct as string) as So_Ct,
    safe_cast(date(m.Ngay_Ct) as date) as Ngay_Ct,
    cast(m.So_Bk as string) as So_Bk,
    cast(m.Sp as string) as Sp,
    safe_cast(m.ID_Tt as int64) as ID_Tt,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(m.Dien_Giai as string) as Dien_Giai,

    safe_cast(date(m.NgayXN) as date) as NgayXN,
    cast(m.UserIDXN as string) as UserIDXN,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    current_timestamp() as UpdateTime

  from {{ source(btmh_source_name(s), 'sldcd') }} d
  left join {{ source(btmh_source_name(s), 'sldcm') }} m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join {{ source(btmh_source_name(s), 'dmkho') }} khox_raw
    on safe_cast(khox_raw.ID as int64) = safe_cast(d.ID_KhoX as int64)

  left join kho_dim khox
    on cast(khox.Ma_Kho as string) = cast(khox_raw.Ma_Kho as string)

  left join {{ source(btmh_source_name(s), 'dmkho') }} khon_raw
    on safe_cast(khon_raw.ID as int64) = safe_cast(d.ID_KhoN as int64)

  left join kho_dim khon
    on cast(khon.Ma_Kho as string) = cast(khon_raw.Ma_Kho as string)

  left join {{ source(btmh_source_name(s), 'dmkho') }} khond_raw
    on safe_cast(khond_raw.ID as int64) = safe_cast(m.ID_KhoN as int64)

  left join kho_dim khond
    on cast(khond.Ma_Kho as string) = cast(khond_raw.Ma_Kho as string)

  left join {{ source(btmh_source_name(s), 'dmdt') }} dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where safe_cast(m.ID_Dv as int64) >= 0
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)
{% endfor %}

select *
from (
{% for s in sources %}
  select * from src_{{ s.code | lower }}
  {% if not loop.last %}union all{% endif %}
{% endfor %}
)

{% if is_incremental() %}
where Ngay >= date_trunc({{ btmh_window_start_date(var('btmh_months_back', 6) | int) }}, month)
{% endif %}
