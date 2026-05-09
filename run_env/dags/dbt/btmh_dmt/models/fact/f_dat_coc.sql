{% set sources = var('dat_coc_sources', []) %}

{% if sources | length == 0 %}
  {% do exceptions.raise_compiler_error("Missing dbt var 'dat_coc_sources' (define it in dbt_project.yml)") %}
{% endif %}

{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    partition_by={"field": "Ngay", "data_type": "date", "granularity": "month"},
    cluster_by=['Nguon', 'Ma_Kho']
  )
}}

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
),

nv_dim as (
  select *
  from {{ ref('d_nhan_vien') }}
)


{% for s in sources %}
, src_{{ s.name }} as (
  with blm_map as (
    select
      safe_cast(blm.Sub_ID as int64) as sub_id,
      max(safe_cast(blm.ID_Dv as int64)) as ID_Dv,
      cast(max(safe_cast(blm.ID as int64)) as string) as ID_Phieu_Thu,
      string_agg(distinct cast(blm.ID as string), ',' order by cast(blm.ID as string)) as ID_Phieu_Thu_All
    from {{ source(btmh_source_name(s), 'slblm') }} blm
    where safe_cast(blm.ID_Dv as int64) >= 0
    group by 1
  )

  select
    '{{ s.company }}' as Nguon,

    cast(m.ID_Dv as int64) as ID_Dv,
    cast(d.ID as string) as ID,
    cast(d.Stt as string) as STT_Tren_Phieu,
    concat(cast(d.ID as string), '-', cast(d.Stt as string), '{{ s.company }}') as Composite_ID,

    {% if s.fixed_ma_kho %}
    '{{ s.fixed_ma_kho }}' as Ma_Kho,
    {% else %}
    cast(kho.Ma_Kho as string) as Ma_Kho,
    {% endif %}

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    safe_cast(d.So_Luong as bignumeric) as So_Luong,
    safe_cast(d.Ty_Gia as bignumeric) as Ty_Gia,
    safe_cast(d.Tong_Tien as bignumeric) as Tong_Tien,

    cast(dt.Ma_Dt as string) as Ma_KH,
    cast(dt.Ma_Dt as string) as Ma_Dt,
    cast(nv.Ma_Nv as string) as Ma_Nv,

    date(m.Ngay) as Ngay,
    date(m.Ngay_Giao) as Ngay_Giao,

    cast(m.Dien_Giai as string) as Dien_Giai,
    safe_cast(m.IsCopy as int64) as IsCopy,
    cast(m.MarkRow as string) as MarkRow,
    cast(m.ID as string) as Sub_ID,

    blm_map.ID_Phieu_Thu as ID_Phieu_Thu,
    blm_map.ID_Phieu_Thu_All as ID_Phieu_Thu_All,

    cast(m.InsertDate as timestamp) as Thoi_diem_tao,
    cast(m.LastEdit as timestamp) as LastEdit,
    current_timestamp() as UpdateTime

  from {{ source(btmh_source_name(s), 'slvbdcd') }} d
  left join {{ source(btmh_source_name(s), 'slvbdcm') }} m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join {{ source(btmh_source_name(s), 'dmkho') }} kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(m.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join {{ source(btmh_source_name(s), 'dmdt') }} dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  left join {{ source(btmh_source_name(s), 'dmnv') }} nv_raw
    on safe_cast(nv_raw.ID as int64) = safe_cast(m.ID_Nv as int64)

  left join nv_dim nv
    on cast(nv.Ma_Nv as string) = cast(nv_raw.Ma_Nv as string)

  left join blm_map
    on blm_map.sub_id = safe_cast(m.ID as int64)

  cross join params p
  where 1=1
    and safe_cast(m.IsCopy as int64) = 1
    and m.MarkRow is null
    {% if is_incremental() %}
      and date(m.Ngay) >= p.start_month
      and date(m.Ngay) < p.end_excl
    {% endif %}
)
{% endfor %}

select * from (
{% for s in sources %}
  select * from src_{{ s.name }}
  {% if not loop.last %}union all{% endif %}
{% endfor %}
)
