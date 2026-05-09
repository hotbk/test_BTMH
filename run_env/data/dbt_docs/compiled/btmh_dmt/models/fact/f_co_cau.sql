

with src as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`{{ env_var('ggs_bq_dataset', 'btmh_stg_ggs') }}`.`ggs_co_cau`
),

sn as (
  select
    cast(s.id_nhom_hang_theo_co_cau as string) as id_dai_chi,
    cast(s.ten_mo_ta as string) as ten_dai_chi,
    cast(s.dai_chi as string) as dai_chi,
    x.ma_ch,
    safe_cast(x.sn as int64) as sn
  from src s
  cross join unnest([
    struct('CH1' as ma_ch, s.ch1_sn_cap_hang as sn),
    struct('CH2' as ma_ch, s.ch2_sn_cap_hang as sn),
    struct('CH3' as ma_ch, s.ch3_sn_cap_hang as sn),
    struct('CH4' as ma_ch, s.ch4_sn_cap_hang as sn),
    struct('CH5' as ma_ch, s.ch5_sn_cap_hang as sn),
    struct('CH6' as ma_ch, s.ch6_sn_cap_hang as sn),
    struct('CH7' as ma_ch, s.ch7_sn_cap_hang as sn),
    struct('CH8' as ma_ch, s.ch8_sn_cap_hang as sn),
    struct('CH9' as ma_ch, s.ch9_sn_cap_hang as sn),
    struct('GH1' as ma_ch, s.gh1_sn_cap_hang as sn),
    struct('TMDT' as ma_ch, s.tmdt_sn_cap_hang as sn),
    struct('HD1' as ma_ch, s.hd1_sn_cap_hang as sn),
    struct('BN1' as ma_ch, s.bn1_sn_cap_hang as sn)
  ]) x
  where x.sn is not null
),

cc as (
  select
    cast(s.id_nhom_hang_theo_co_cau as string) as id_dai_chi,
    cast(s.ten_mo_ta as string) as ten_dai_chi,
    cast(s.dai_chi as string) as dai_chi,
    x.ma_ch,
    safe_cast(x.cc as int64) as cc
  from src s
  cross join unnest([
    struct('CH1' as ma_ch, s.ch1_co_cau_1 as cc),
    struct('CH2' as ma_ch, s.ch2_co_cau_1 as cc),
    struct('CH3' as ma_ch, s.ch3_co_cau_1 as cc),
    struct('CH4' as ma_ch, s.ch4_co_cau_1 as cc),
    struct('CH5' as ma_ch, s.ch5_co_cau_1 as cc),
    struct('CH6' as ma_ch, s.ch6_co_cau_1 as cc),
    struct('CH7' as ma_ch, s.ch7_co_cau_1 as cc),
    struct('CH8' as ma_ch, s.ch8_co_cau_1 as cc),
    struct('CH9' as ma_ch, s.ch9_co_cau_1 as cc),
    struct('GH1' as ma_ch, s.gh1_co_cau_1 as cc),
    struct('TMDT' as ma_ch, s.tmdt_co_cau_1 as cc),
    struct('HD1' as ma_ch, s.hd1_co_cau_1 as cc),
    struct('BN1' as ma_ch, s.bn1_co_cau_1 as cc),
    struct('KPPCU' as ma_ch, s.kppcu_co_cau_1 as cc)
  ]) x
  where x.cc is not null
)

select
  concat(coalesce(sn.id_dai_chi, cc.id_dai_chi), '_', coalesce(sn.ma_ch, cc.ma_ch)) as Composite_ID,
  coalesce(sn.id_dai_chi, cc.id_dai_chi) as id_dai_chi,
  coalesce(sn.ten_dai_chi, cc.ten_dai_chi) as ten_dai_chi,
  coalesce(sn.dai_chi, cc.dai_chi) as dai_chi,
  coalesce(sn.ma_ch, cc.ma_ch) as ma_ch,
  coalesce(sn.sn, 0) as sn,
  coalesce(cc.cc, 0) as cc
from sn
full outer join cc
  on cc.id_dai_chi = sn.id_dai_chi
  and cc.ma_ch = sn.ma_ch