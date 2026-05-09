{% set sources = var('slbl_sources', []) %}

{% if sources | length == 0 %}
  {% do exceptions.raise_compiler_error("Missing dbt var 'slbl_sources' (define it in dbt_project.yml)") %}
{% endif %}

{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    partition_by={"field": "Ngay", "data_type": "date", "granularity": "month"},
    cluster_by=['Ma_Cong_Ty', 'Ma_Kho']
  )
}}

with params as (
  select
    {{ btmh_window_end_date() }} as run_date,
    {{ btmh_window_start_date(var('btmh_months_back', 6) | int) }} as start_date,
    date_trunc({{ btmh_window_start_date(var('btmh_months_back', 6) | int) }}, month) as start_month,
    date_add(date_trunc({{ btmh_window_end_date() }}, month), interval 1 month) as end_excl,
    date '2025-08-31' as hd_legacy_end
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
),

the_dedup as (
  select * except(_rn)
  from (
    select
      the.*,
      row_number() over (
        partition by cast(the.Nguon as string), safe_cast(the.ID as int64)
        order by datetime(coalesce(the.LastEdit, the.InsertDate)) desc
      ) as _rn
    from {{ ref('d_the') }} the
  )
  where _rn = 1
),


{% for s in sources %}
src_{{ s.name }} as (
  with
  vou as (
    select
      vou.*,
      row_number() over (
        partition by safe_cast(vou.ID as int64)
        order by safe_cast(vou.Stt as int64)
      ) as _rn
    from {{ source(btmh_source_name(s), 'slbld') }} vou
    where cast(vou.Hs_Qd as string) = 'VOUCHER'
  )

  select
    concat(cast(m.ID as string), '-', cast(d.Stt as string), '{{ s.company }}') as ID,
    '{{ s.company }}' as Ma_Cong_Ty,
    safe_cast(m.ID_Dv as int64) as ID_Dv,
    safe_cast(m.ID as int64) as ID_Phieu_thu,
    date(m.Ngay) as Ngay,
    cast(m.Ngay as datetime) as Ngay_PhieuThu,
    safe_cast(d.Stt as int64) as STT_TrenPhieu,

    safe_cast(d.ID_Hang as int64) as ID_Hang,

    safe_cast(d.Md as int64) as Ma_dong,

    safe_cast(d.So_Luong as bignumeric) as So_Luong,

    (
      {{
        btmh_calc_slbl_qty(
          "safe_cast(d.So_Luong as bignumeric)",
          "cast(nh.Ma_Nhom as string)",
          "cast(h.Ma_Hang as string)",
          "coalesce(safe_cast(h.T_Luong as bignumeric), 0)",
          "coalesce(safe_cast(h.The_Tich as bignumeric), 0)",
          "coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)",
          "coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)"
        )
      }}
    ) as SL_Thuc_Te,

    safe_cast(d.Sl_Qd as bignumeric) as Sl_Qd,

    (
      {{
        btmh_calc_slbl_qty(
          "safe_cast(d.So_Luong as bignumeric)",
          "cast(nh.Ma_Nhom as string)",
          "cast(h.Ma_Hang as string)",
          "coalesce(safe_cast(h.T_Luong as bignumeric), 0)",
          "coalesce(safe_cast(h.The_Tich as bignumeric), 0)",
          "coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)",
          "coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)"
        )
      }}
    ) as SL_Chi_TT,

    safe_cast(d.Don_Gia as bignumeric) as Don_gia_ban,
    safe_cast(d.T_Tien as bignumeric) as Thanh_tien_theo_DG_ban,
    safe_cast(d.T_Tien1 as bignumeric) as Gia_Von,

    safe_cast(d.TyLe_Giam as bignumeric) as TyLe_Giam_Gia_TP,
    safe_cast(d.Tien_Giam as bignumeric) as Tien_giam_gia_TP,
    safe_cast(d.Tien_CK as bignumeric) as CK_phan_bo,
    safe_cast(d.CK_The as bignumeric) as CK_the_phan_bo,
    safe_cast(d.CK_TheCn as bignumeric) as CK_TheCn_phan_bo,
    safe_cast(d.CK_TheMg as bignumeric) as CK_TheMg_phan_bo,

    (
      safe_cast(d.T_Tien as bignumeric)
      - (
        coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
        + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      )
    ) as Tien_PhieuThu1,

    (
      coalesce(safe_cast(d.Tien_Giam as bignumeric), 0)
      + coalesce(safe_cast(d.Tien_CK as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheCn as bignumeric), 0)
      + coalesce(safe_cast(d.CK_TheMg as bignumeric), 0)
    ) as Tien_Chiet_Khau,

    cast(m.Dien_Giai as string) as Dien_giai_Phieu_thu,

    {% if s.fixed_ma_kho %}
    '{{ s.fixed_ma_kho }}' as Ma_Kho,
    {% else %}
    cast(kho.Ma_Kho as string) as Ma_Kho,
    {% endif %}

    cast(m.Quay as string) as Quay,

    cast(nv.Ma_Nv as string) as Ma_Nv,
    cast(nvkt.Ma_Nv as string) as Ma_NvKT,

    cast(dt.Ma_Dt as string) as Ma_KH,
    cast(dt.Ma_Dt as string) as Ma_Dt,

    cast(csb_d.Ky_Hieu as string) as ctkm_kh,
    cast(csb_d.Noi_Dung as string) as ctkm_nd,
    cast(csb.Ky_Hieu as string) as ctkm_kh_pt,
    cast(csb.Noi_Dung as string) as ctkm_nd_pt,

    cast(the.So_The as string) as so_the_voucher,
    cast(the.Ten_The as string) as ten_the_voucher,

    cast(d.No_Tk as string) as No_Tk,
    cast(d.Co_Tk as string) as Co_Tk,

    cast(m.InsertDate as timestamp) as InsertDate,
    cast(m.Dc_Giao as string) as Dc_Giao,
    cast(m.Sub_ID as string) as Sub_ID,

    m.MarkRow,

    current_timestamp() as UpdateTime

  from {{ source(btmh_source_name(s), 'slbld') }} d
  left join {{ source(btmh_source_name(s), 'slblm') }} m
    on safe_cast(m.ID as int64) = safe_cast(d.ID as int64)

  left join dmh h
    on safe_cast(h.ID as int64) = safe_cast(d.ID_Hang as int64)

  left join {{ ref('d_nhom') }} nh
    on safe_cast(nh.ID as int64) = safe_cast(h.ID_Nhom as int64)

  left join {{ source(btmh_source_name(s), 'dmkho') }} kho_raw
    on safe_cast(kho_raw.ID as int64) = safe_cast(m.ID_Kho as int64)

  left join kho_dim kho
    on cast(kho.Ma_Kho as string) = cast(kho_raw.Ma_Kho as string)

  left join {{ source(btmh_source_name(s), 'dmnv') }} nv_raw
    on safe_cast(nv_raw.ID as int64) = safe_cast(m.ID_Nv as int64)

  left join nv_dim nv
    on cast(nv.Ma_Nv as string) = cast(nv_raw.Ma_Nv as string)

  left join {{ source(btmh_source_name(s), 'dmnv') }} nvkt_raw
    on safe_cast(nvkt_raw.ID as int64) = safe_cast(m.UserIDXN as int64)

  left join nv_dim nvkt
    on cast(nvkt.Ma_Nv as string) = cast(nvkt_raw.Ma_Nv as string)

  left join {{ source(btmh_source_name(s), 'dmdt') }} dt_raw
    on safe_cast(dt_raw.ID as int64) = safe_cast(m.ID_Dt as int64)

  left join dt_dim dt
    on cast(dt.Ma_Dt as string) = cast(dt_raw.Ma_Dt as string)

  left join {{ source(btmh_source_name(s), 'csb') }} csb
    on safe_cast(csb.ID as int64) = safe_cast(m.ID_CSB as int64)

  left join {{ source(btmh_source_name(s), 'csb') }} csb_d
    on safe_cast(csb_d.ID as int64) = safe_cast(d.ID_CSB as int64)

  left join vou
    on safe_cast(vou.ID as int64) = safe_cast(d.ID as int64)
    and vou._rn = 1
    and safe_cast(d.ID_CSB as int64) is not null
    and safe_cast(m.ID_CSB as int64) != safe_cast(d.ID_CSB as int64)

  left join the_dedup the
    on safe_cast(the.ID as int64) = safe_cast(vou.ID_The as int64)
    and cast(the.Nguon as string) = '{{ s.company }}'

  cross join params p
  where 1=1
    {% if is_incremental() %}
      and date(m.Ngay) >= p.start_month
      and date(m.Ngay) < p.end_excl
    {% endif %}

    {% if s.name == 'hd1' %}
    and date(m.Ngay) >= p.hd_legacy_end
    {% endif %}

    {% if s.ngay_end_date %}
    and date(m.Ngay) < {{ s.ngay_end_date }}
    {% endif %}

    {% if s.name == 'hd2' %}
    and m.UserIDXN is null
    {% endif %}

    {% if s.check_user_idxn %}
    and m.UserIDXN is not null
    {% endif %}
)
{% if not loop.last %},{% endif %}
{% endfor %}

select * from (
{% for s in sources %}
  select * from src_{{ s.name }}
  {% if not loop.last %}union all{% endif %}
{% endfor %}
)
