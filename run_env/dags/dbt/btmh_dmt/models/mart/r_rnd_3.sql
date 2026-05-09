{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='MA_MAU',
    on_schema_change='sync_all_columns'
  )
}}

{% set src_schema = (var('d_hang_schema', 'stg_augges_225') | lower) %}

with vbmm as (
  select *
  from {{ source(src_schema, 'dmvbmm') }}
),

bost as (
  select * from vbmm where cast(cDM as string) = 'BOST'
),
nhomct as (
  select * from vbmm where cast(cDM as string) = 'NHOMCT'
),
chungl as (
  select * from vbmm where cast(cDM as string) = 'CHUNGL'
),
gioit as (
  select * from vbmm where cast(cDM as string) = 'GIOIT'
),
hoatiet as (
  select * from vbmm where cast(cDM as string) = 'HOATIET'
),
loaida as (
  select * from vbmm where cast(cDM as string) = 'LOAIDA'
),
tendc as (
  select * from vbmm where cast(cDM as string) = 'TENDC'
),
hamlkl as (
  select * from vbmm where cast(cDM as string) = 'HAMLKL'
),

base as (
  select
    cast(dmnmm.Ma_NM as string) as MA_MAU,
    cast(dmnh.Ma_Nhom as string) as NHOM_HANG,
    cast(dmnmm.ngay_tao_ma_mau as datetime) as NGAY_TAO_MA_MAU,
    cast(dmnmm.Anh_NM as string) as ANH_MA_MAU,

    cast(bost.Ten as string) as BO_SUU_TAP,
    cast(nhomct.Ten as string) as DANH_MUC_SAN_PHAM,
    cast(chungl.Ten as string) as CHUNG_LOAI,
    cast(gioit.Ten as string) as GIOI_TINH_SAN_PHAM,
    cast(hoatiet.Ten as string) as HOA_TIET_MAT,
    cast(loaida.Ten as string) as LOAI_DA,
    cast(tendc.Ten as string) as TEN_DA_CHU,
    cast(hamlkl.Ten as string) as HAM_LUONG_KIM_LOAI

  from {{ ref('d_hang') }} dmh
  left join {{ ref('d_nhom') }} dmnh
    on safe_cast(dmnh.ID as int64) = safe_cast(dmh.ID_Nhom as int64)
  left join {{ ref('d_nhom_ma_mau') }} dmnmm
    on safe_cast(dmnmm.ID as int64) = safe_cast(dmh.ID_NMM as int64)

  left join bost
    on safe_cast(dmnmm.ID_BoST as int64) = safe_cast(bost.ID_Stt as int64)
  left join nhomct
    on safe_cast(dmnmm.ID_NhomCt as int64) = safe_cast(nhomct.ID_Stt as int64)
  left join chungl
    on safe_cast(dmnmm.ID_ChungL as int64) = safe_cast(chungl.ID_Stt as int64)
  left join gioit
    on safe_cast(dmnmm.ID_GioiT as int64) = safe_cast(gioit.ID_Stt as int64)
  left join hoatiet
    on safe_cast(dmnmm.ID_HoaTiet as int64) = safe_cast(hoatiet.ID_Stt as int64)
  left join loaida
    on safe_cast(dmnmm.ID_LoaiDa as int64) = safe_cast(loaida.ID_Stt as int64)
  left join tendc
    on safe_cast(dmnmm.ID_TenDC as int64) = safe_cast(tendc.ID_Stt as int64)
  left join hamlkl
    on safe_cast(dmnmm.ID_HamLKL as int64) = safe_cast(hamlkl.ID_Stt as int64)

  where dmnmm.Ma_NM is not null
),

final as (
  select
    MA_MAU,
    MA_MAU as `MA MAU`,
    max(NHOM_HANG) as `NHOM HANG`,
    max(NGAY_TAO_MA_MAU) as `NGAY TAO MA MAU`,
    max(ANH_MA_MAU) as `ANH MA MAU`,

    max(BO_SUU_TAP) as `BO SUU TAP`,
    max(DANH_MUC_SAN_PHAM) as `DANH MUC SAN PHAM`,
    max(CHUNG_LOAI) as `CHUNG LOAI`,
    max(GIOI_TINH_SAN_PHAM) as `GIOI TINH SAN PHAM`,
    max(HOA_TIET_MAT) as `HOA TIET MAT`,
    max(LOAI_DA) as `LOAI DA`,
    max(TEN_DA_CHU) as `TEN DA CHU`,
    max(HAM_LUONG_KIM_LOAI) as `HAM LUONG KIM LOAI`,

    current_timestamp() as UpdateTime

  from base
  group by MA_MAU
)

select *
from final
