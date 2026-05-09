{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    partition_by={"field": "Ngay", "data_type": "date", "granularity": "month"},
    cluster_by=['Nguon', 'Ma_Kho', 'Ma_HT']
  )
}}

{% set sources = var('nhap_xuat_sources', []) %}
{% if sources | length == 0 %}
  {% do exceptions.raise_compiler_error("Missing dbt var 'nhap_xuat_sources' (define it in dbt_project.yml)") %}
{% endif %}

with params as (
  select
    {#
      Support bounded backfills from Airflow:
      - if vars snapshot_start_date/snapshot_end_date are provided, use them (preferred)
      - else if vars start_date/end_date are provided, use them
      - otherwise fall back to a rolling window based on run_date
    #}
    (
      {% if var('snapshot_end_date', '') %}
        date({{ var('snapshot_end_date') | tojson }})
      {% elif var('end_date', '') %}
        date({{ var('end_date') | tojson }})
      {% else %}
        {{ btmh_window_end_date() }}
      {% endif %}
    ) as end_date,
    (
      {% if var('snapshot_start_date', '') %}
        date({{ var('snapshot_start_date') | tojson }})
      {% elif var('start_date', '') %}
        date({{ var('start_date') | tojson }})
      {% else %}
        {{ btmh_window_start_date(var('btmh_months_back', 6) | int) }}
      {% endif %}
    ) as start_date,
    date_trunc(
      (
        {% if var('snapshot_start_date', '') %}
          date({{ var('snapshot_start_date') | tojson }})
        {% elif var('start_date', '') %}
          date({{ var('start_date') | tojson }})
        {% else %}
          {{ btmh_window_start_date(var('btmh_months_back', 6) | int) }}
        {% endif %}
      ),
      month
    ) as start_month,
    date_add(date_trunc(
      (
        {% if var('snapshot_end_date', '') %}
          date({{ var('snapshot_end_date') | tojson }})
        {% elif var('end_date', '') %}
          date({{ var('end_date') | tojson }})
        {% else %}
          {{ btmh_window_end_date() }}
        {% endif %}
      ),
      month
    ), interval 1 month) as end_excl
),

dmh as (
  select *
  from {{ ref('d_hang') }}
),

ht as (
  select *
  from {{ ref('d_hach_toan') }}
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
    '{{ s.code }}' as Nguon,

    safe_cast(m.ID_Dv as int64) as ID_Dv,

    cast(d.ID as string) as ID,
    safe_cast(m.ID_Nx as int64) as ID_Nx,
    cast(ht.Ma_HT as string) as Ma_HT,
    safe_cast(d.ID_Hang as int64) as ID_Hang,

    (
      coalesce(safe_cast(h.T_Luong as bignumeric), 0)
      + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
      + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
      + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
    ) as Tong_tlg,

    (
      {{
        btmh_calc_slbl_qty(
          "safe_cast(d.So_Luong as bignumeric)",
          "cast(h.Ma_Nhom as string)",
          "cast(h.Ma_Hang as string)",
          "coalesce(safe_cast(h.T_Luong as bignumeric), 0)",
          "coalesce(safe_cast(h.The_Tich as bignumeric), 0)",
          "coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)",
          "coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)"
        )
      }}
    ) as SL_Chi_TT,

    safe_cast(d.So_Luong as bignumeric) as So_Luong_Theo_Dvt,
    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    safe_cast(d.Gia_Ban as bignumeric) as Gia_Ban,
    safe_cast(d.Gia_VAT as bignumeric) as Gia_VAT,
    safe_cast(d.Gia_KVat as bignumeric) as Gia_KVat,
    safe_cast(d.Gia_Qd as bignumeric) as Gia_Qd,
    safe_cast(d.Don_Gia as bignumeric) as Don_Gia,
    safe_cast(d.T_Tien as bignumeric) as T_Tien,

    safe_cast(d.Don_Gia1 as bignumeric) as Don_Gia1,
    safe_cast(d.T_Tien1 as bignumeric) as T_Tien1,

    date(m.Ngay) as Ngay,
    date(m.Ngay_Ct) as Ngay_Ct,
    cast(m.Sp as string) as Sp,

    (
      case
        when '{{ s.code }}' = 'HD' and cast(kho.Ma_Kho as string) = 'CH1' then 'HD1'
        when '{{ s.code }}' = 'HD' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMHD1'
        when '{{ s.code }}' = 'BN' and cast(kho.Ma_Kho as string) = 'CH1' then 'BN1'
        when '{{ s.code }}' = 'BN' and cast(kho.Ma_Kho as string) = 'KM01' then 'KMBN1'
        else cast(kho.Ma_Kho as string)
      end
    ) as Ma_Kho,

    cast(dt.Ma_Dt as string) as Ma_Dt,
    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,
    cast(m.Sub_ID as string) as Sub_ID,

    cast(m.So_Ct as string) as So_Ct,

    current_timestamp() as UpdateTime

  from {{ source(btmh_source_name(s), 'slnxd') }} d
  left join {{ source(btmh_source_name(s), 'slnxm') }} m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join ht
    on safe_cast(ht.ID as int64) = safe_cast(m.ID_Nx as int64)
    and cast(ht.Nguon as string) = '{{ s.code }}'

  left join {{ source(btmh_source_name(s), 'dmkho') }} kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(d.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join {{ source(btmh_source_name(s), 'dmdt') }} dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  cross join params p
  where 1=1
    and d.ID_Hang is not null
    and date(m.Ngay) >= p.start_month
    and date(m.Ngay) < p.end_excl
)
{% endfor %}

select t.* from (
{% for s in sources %}
  select * from src_{{ s.code | lower }}
  {% if not loop.last %}union all{% endif %}
{% endfor %}
) t
cross join params p

{% if is_incremental() %}
where t.Ngay >= p.start_month
{% endif %}
