{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    partition_by={"field": "Ngay", "data_type": "date"},
  )
}}

with params as (
  select
    {{ btmh_run_date() }} as run_date,
    {{ btmh_start_date(2) }} as start_date
)

select Ngay
      ,Nguon
      ,Ma_Kho
      ,nx.ID_Hang
      ,h.Nganhhang_fix as Nganh_hang
      ,h.Dongsp_fix as Dong_Sp
      ,h.Ten_Hang
      ,sum(So_Luong_Theo_Dvt) as quantity_by_unit
      ,sum(SL_Chi_TT) as quantity
      ,sum(T_Tien) as line_income
from {{ ref('f_nhap_xuat') }} nx left join {{ ref('d_hang_agg') }} h on nx.ID_Hang = h.ID 
where 1=1
and ID_Dv >= 0
and Ma_HT = 'NLNO'

{% if is_incremental() %}
  and nx.Ngay >= (select start_date from params)
  and nx.Ngay <= (select run_date from params)
{% endif %}

group by 1,2,3,4,5,6,7
