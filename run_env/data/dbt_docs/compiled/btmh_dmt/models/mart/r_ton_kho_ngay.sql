








with


params as (
  
    select 
(
  
    current_date()
  
)
 as p_date
  
),

-- 2.1 SL_HT: tồn kho tại ngày
sl_ht as (
  select
    cast(tk.nguon as string) as Nguon,
    cast(tk.ma_kho as string) as Ma_Kho,
    safe_cast(tk.id_hang as int64) as ID_Hang,
    sum(coalesce(safe_cast(tk.so_luong_ton as bignumeric), 0)) as so_luong_ton,
    sum(coalesce(safe_cast(tk.gia_tri_ton as bignumeric), 0)) as gia_tri_ton
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_ton_kho` tk
  cross join params p
  where date(tk.ngay) = p.p_date
    and (
      substr(cast(tk.ma_kho as string), 1, 2) in ('CH', 'ST', 'KM', 'GH')
      or cast(tk.ma_kho as string) in (
        'FS01', 'PO01', 'GH9', 'GHTL', 'KPP', 'KPPCU', 'KCU', 'TMDT', 'B2B',
        'BN1', 'HD1', 'XKTP', 'XKTT', 'B2BBL'
      )
    )
  group by 1, 2, 3
),

-- 2.2 DCh: điều chuyển về SHP trong ngày

dch as (
  select
    cast(dc.Nguon as string) as Nguon,
    cast(dc.Ma_KhoND as string) as Ma_Kho,
    safe_cast(dc.ID_Hang as int64) as ID_Hang,
    sum(coalesce(safe_cast(dc.So_luong as bignumeric), 0)) as Dieu_Chuyen_SHP
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_dieu_chuyen` dc
  cross join params p
  where date(dc.Ngay) = p.p_date
    and cast(dc.Ma_KhoN as string) = 'SHP'
    and (
      substr(cast(dc.Ma_KhoND as string), 1, 2) in ('CH', 'ST', 'KM', 'GH')
      or cast(dc.Ma_KhoND as string) in (
        'FS01', 'PO01', 'GH9', 'GHTL', 'KPP', 'KPPCU', 'KCU', 'TMDT', 'B2B',
        'BN1', 'HD1', 'XKTP', 'XKTT', 'B2BBL'
      )
    )
  group by 1, 2, 3
),

-- 2.3 DT: doanh thu trong ngày

dt as (
  select
    cast(dt.Ma_Cong_Ty as string) as Nguon,
    cast(dt.Ma_Kho as string) as Ma_Kho,
    safe_cast(dt.ID_Hang as int64) as ID_Hang,
    sum(
      case
        when dt.Sub_ID is null or safe_cast(dt.Sub_ID as int64) = 0
          then coalesce(safe_cast(dt.So_Luong as bignumeric), 0)
        else 0
      end
    ) as San_luong_ban,
    sum(
      case
        when dt.Sub_ID is not null and safe_cast(dt.Sub_ID as int64) != 0
          then coalesce(safe_cast(dt.So_Luong as bignumeric), 0)
        else 0
      end
    ) as San_luong_ban_KD
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_doanh_thu` dt
  cross join params p
  where date(dt.Ngay_PhieuThu) = p.p_date
    and coalesce(safe_cast(dt.ID_Dv as int64), 0) = 0
    and coalesce(safe_cast(dt.So_Luong as bignumeric), 0) > 0
    and coalesce(safe_cast(dt.Thanh_tien_theo_DG_ban as bignumeric), 0) > 0
    and (
      dt.Dien_giai_Phieu_thu is null
      or (
        trim(upper(cast(dt.Dien_giai_Phieu_thu as string))) != '#DTT#'
        and trim(upper(cast(dt.Dien_giai_Phieu_thu as string))) != '#ĐTT#'
      )
    )
  group by 1, 2, 3
),

-- 2.4 DC: đặt cọc (lũy kế + tại ngày)

dc_src as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_dat_coc`
  where coalesce(safe_cast(ID_Dv as int64), 0) = 0
    and (ID_Phieu_Thu is null or trim(cast(ID_Phieu_Thu as string)) = '')
),

dc as (
  select
    cast(dc.Nguon as string) as Nguon,
    cast(dc.Ma_Kho as string) as Ma_Kho,
    safe_cast(dc.ID_Hang as int64) as ID_Hang,
    sum(coalesce(safe_cast(dc.So_Luong as bignumeric), 0)) as So_Luong_Tong,
    sum(
      case
        when date(dc.Ngay) = p.p_date then coalesce(safe_cast(dc.So_Luong as bignumeric), 0)
        else 0
      end
    ) as So_Luong_Tai_Ngay
  from dc_src dc
  cross join params p
  where date(dc.Ngay) <= p.p_date
  group by 1, 2, 3
),

-- 2.5 DC_Giao: dự kiến giao T..T+5
dcg as (
  select
    cast(dc.Nguon as string) as Nguon,
    cast(dc.Ma_Kho as string) as Ma_Kho,
    safe_cast(dc.ID_Hang as int64) as ID_Hang,

    sum(case when date(dc.Ngay_Giao) <= p.p_date then coalesce(safe_cast(dc.So_Luong as bignumeric), 0) else 0 end) as So_Luong_T,
    sum(case when date(dc.Ngay_Giao) <= date_add(p.p_date, interval 1 day) then coalesce(safe_cast(dc.So_Luong as bignumeric), 0) else 0 end) as So_Luong_T1,
    sum(case when date(dc.Ngay_Giao) <= date_add(p.p_date, interval 2 day) then coalesce(safe_cast(dc.So_Luong as bignumeric), 0) else 0 end) as So_Luong_T2,
    sum(case when date(dc.Ngay_Giao) <= date_add(p.p_date, interval 3 day) then coalesce(safe_cast(dc.So_Luong as bignumeric), 0) else 0 end) as So_Luong_T3,
    sum(case when date(dc.Ngay_Giao) <= date_add(p.p_date, interval 4 day) then coalesce(safe_cast(dc.So_Luong as bignumeric), 0) else 0 end) as So_Luong_T4,
    sum(case when date(dc.Ngay_Giao) <= date_add(p.p_date, interval 5 day) then coalesce(safe_cast(dc.So_Luong as bignumeric), 0) else 0 end) as So_Luong_T5

  from dc_src dc
  cross join params p
  where date(dc.Ngay_Giao) <= date_add(p.p_date, interval 5 day)
  group by 1, 2, 3
),

-- 2.6 HML: hàng mua lại trong ngày (removed)

-- 2.7 CC: cơ cấu (max cc)

cc as (
  select
    cast(id_dai_chi as string) as ID_Dai_Chi,
    cast(ma_ch as string) as Ma_Kho,
    max(coalesce(safe_cast(cc as int64), 0)) as Sl_CC
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_co_cau`
  group by 1, 2
),

-- 3) Full join SL_HT & DCh

main as (
  select
    coalesce(sl_ht.Nguon, dch.Nguon) as Nguon,
    coalesce(sl_ht.Ma_Kho, dch.Ma_Kho) as Ma_Kho,
    coalesce(sl_ht.ID_Hang, dch.ID_Hang) as ID_Hang,
    sl_ht.so_luong_ton,
    sl_ht.gia_tri_ton,
    dch.Dieu_Chuyen_SHP
  from sl_ht
  full outer join dch
    on sl_ht.Nguon = dch.Nguon
    and sl_ht.Ma_Kho = dch.Ma_Kho
    and sl_ht.ID_Hang = dch.ID_Hang
),

-- Dim + vbmm lookups

vbmm as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmvbmm`
),

joined as (
  select
    m.Nguon,
    p.p_date as Ngay,
    m.Ma_Kho,
    kho.Ten_Kho,

    (
      case
        when h.Ma_Hang in ('NLVRTL','NL999','NL9999','NLKGB','NL999KD','NL9999KD','NL06')
          then h.Ma_Hang
        when nmm.Ma_NM in ('NLDXV49KD0-505001','NLKGV24KD0-208001')
          then nmm.Ma_NM
        else h.Ten_HangN
      end
    ) as ID_DaiChi,

    (
      case
        when h.Ma_Hang in ('NLVRTL','NL999','NL9999','NLKGB','NL999KD','NL9999KD','NL06')
          then h.Ten_Hang
        when nmm.Ma_NM in ('NLDXV49KD0-505001','NLKGV24KD0-208001')
          then nmm.Mo_Ta
        when h.Ten_Dai_Chi is not null
          then h.Ten_Dai_Chi
        else nmm.Mo_Ta
      end
    ) as Ten_Dai_Chi,

    h.Dai_Chi,
    nh.Ma_Nhom as Nhom_hang,
    h.dong_san_pham,

    nmm.Ma_NM as Ma_mau,
    chungl.Ten as Chung_loai,
    gioit.Ten as Gioi_tinh_ma_mau,
    hamlkl.Ten as Ham_luong_kim_loai,
    maubmkl.Ten as Mau_sac,

    m.so_luong_ton,
    m.gia_tri_ton,
    m.Dieu_Chuyen_SHP,

    dt.San_luong_ban,
    dt.San_luong_ban_KD,

    dc.So_Luong_Tong,
    dc.So_Luong_Tai_Ngay,

    dcg.So_Luong_T,
    dcg.So_Luong_T1,
    dcg.So_Luong_T2,
    dcg.So_Luong_T3,
    dcg.So_Luong_T4,
    dcg.So_Luong_T5,

    cast(0 as bignumeric) as So_Luong_Mua,
    cast(0 as bignumeric) as T_Luong_Mua,

    cc.Sl_CC,

    h.T_Luong,
    h.The_Tich,
    h.Tien_Lai,
    h.Tyle_Lai,

    current_timestamp() as UpdateTime

  from main m
  cross join params p

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_kho` kho
    on cast(m.Ma_Kho as string) = cast(kho.Ma_Kho as string)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` h
    on safe_cast(m.ID_Hang as int64) = safe_cast(h.ID as int64)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` nh
    on safe_cast(h.ID_Nhom as int64) = safe_cast(nh.ID as int64)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom_ma_mau` nmm
    on safe_cast(h.ID_NMM as int64) = safe_cast(nmm.ID as int64)

  left join dt
    on m.Nguon = dt.Nguon and m.Ma_Kho = dt.Ma_Kho and m.ID_Hang = dt.ID_Hang

  left join dc
    on m.Nguon = dc.Nguon and m.Ma_Kho = dc.Ma_Kho and m.ID_Hang = dc.ID_Hang

  left join dcg
    on m.Nguon = dcg.Nguon and m.Ma_Kho = dcg.Ma_Kho and m.ID_Hang = dcg.ID_Hang

  left join cc
    on cast(h.ID_Dai_Chi as string) = cast(cc.ID_Dai_Chi as string)
    and cast(m.Ma_Kho as string) = cast(cc.Ma_Kho as string)

  left join vbmm chungl
    on safe_cast(nmm.ID_ChungL as int64) = safe_cast(chungl.ID_Stt as int64)
    and cast(chungl.cDM as string) = 'CHUNGL'

  left join vbmm gioit
    on safe_cast(nmm.ID_GioiT as int64) = safe_cast(gioit.ID_Stt as int64)
    and cast(gioit.cDM as string) = 'GIOIT'

  left join vbmm hamlkl
    on safe_cast(nmm.ID_HamLKL as int64) = safe_cast(hamlkl.ID_Stt as int64)
    and cast(hamlkl.cDM as string) = 'HAMLKL'

  left join vbmm maubmkl
    on safe_cast(nmm.ID_MauBMKL as int64) = safe_cast(maubmkl.ID_Stt as int64)
    and cast(maubmkl.cDM as string) = 'MAUBMKL'
)

select
  Nguon,
  Ngay,
  Ma_Kho,
  Ten_Kho,
  ID_DaiChi,
  Ten_Dai_Chi,
  Dai_Chi,
  Nhom_hang,
  dong_san_pham,
  Ma_mau,
  Chung_loai,
  Gioi_tinh_ma_mau,
  Ham_luong_kim_loai,
  Mau_sac,

  -- Metrics
  coalesce(safe_cast(so_luong_ton as bignumeric), 0) as Sl_Ton,
  (coalesce(safe_cast(so_luong_ton as bignumeric), 0) * coalesce(safe_cast(T_Luong as bignumeric), 0)) as Trong_luong_ton,
  (
    coalesce(safe_cast(so_luong_ton as bignumeric), 0)
    * (
      coalesce(safe_cast(T_Luong as bignumeric), 0)
      + coalesce(safe_cast(The_Tich as bignumeric), 0)
      + coalesce(safe_cast(Tien_Lai as bignumeric), 0)
      + coalesce(safe_cast(Tyle_Lai as bignumeric), 0)
    )
  ) as Tong_trong_luong_ton,

  coalesce(safe_cast(Sl_CC as bignumeric), 0) as Sl_CC,

  coalesce(safe_cast(San_luong_ban_KD as bignumeric), 0) as San_luong_ban_KD,
  coalesce(safe_cast(San_luong_ban as bignumeric), 0) as San_luong_ban,
  (coalesce(safe_cast(San_luong_ban_KD as bignumeric), 0) * coalesce(safe_cast(T_Luong as bignumeric), 0)) as Trong_luong_ban_KD,
  (coalesce(safe_cast(San_luong_ban as bignumeric), 0) * coalesce(safe_cast(T_Luong as bignumeric), 0)) as Trong_luong_ban,

  coalesce(safe_cast(So_Luong_Tong as bignumeric), 0) as Dat_Coc,
  (coalesce(safe_cast(So_Luong_Tong as bignumeric), 0) * coalesce(safe_cast(T_Luong as bignumeric), 0)) as Trong_Luong_Dat_Coc,
  coalesce(safe_cast(So_Luong_Tai_Ngay as bignumeric), 0) as Dat_Coc_Tai_Ngay,
  (coalesce(safe_cast(So_Luong_Tai_Ngay as bignumeric), 0) * coalesce(safe_cast(T_Luong as bignumeric), 0)) as Trong_Luong_Dat_Coc_Tai_Ngay,

  coalesce(safe_cast(So_Luong_T as bignumeric), 0) as Dat_Coc_Tra_T,
  coalesce(safe_cast(So_Luong_T1 as bignumeric), 0) as Dat_Coc_Tra_T1,
  coalesce(safe_cast(So_Luong_T2 as bignumeric), 0) as Dat_Coc_Tra_T2,
  coalesce(safe_cast(So_Luong_T3 as bignumeric), 0) as Dat_Coc_Tra_T3,
  coalesce(safe_cast(So_Luong_T4 as bignumeric), 0) as Dat_Coc_Tra_T4,
  coalesce(safe_cast(So_Luong_T5 as bignumeric), 0) as Dat_Coc_Tra_T5,

  coalesce(safe_cast(So_Luong_Mua as bignumeric), 0) as Mua_Lai,
  coalesce(safe_cast(T_Luong_Mua as bignumeric), 0) as Trong_luong_Mua_Lai,

  coalesce(safe_cast(gia_tri_ton as bignumeric), 0) as Gia_Tri,
  coalesce(safe_cast(Dieu_Chuyen_SHP as bignumeric), 0) as Dieu_Chuyen_SHP,

  UpdateTime

from joined

