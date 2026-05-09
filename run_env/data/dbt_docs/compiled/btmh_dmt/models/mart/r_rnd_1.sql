



with params as (
  select
    
(
  
    current_date()
  
)
 as report_date,
    date('2024-01-01') as backfill_start_date
),

vbmm as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmvbmm`
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
bost as (
  select * from vbmm where cast(cDM as string) = 'BOST'
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

map_channel as (
  select
    k as raw_ma_kho,
    case
      when k in ('CH1','KM01','GH1') then 'CH1'
      when k in ('CH2','KM02','GH2') then 'CH2'
      when k in ('CH3','KM03','GH3') then 'CH3'
      when k in ('CH4','KM04','GH4') then 'CH4'
      when k in ('CH5','KM05','GH5') then 'CH5'
      when k in ('CH6','KM06','GH6') then 'CH6'
      when k in ('CH7','KM07','GH7') then 'CH7'
      when k in ('CH8','KM08','GH8') then 'CH8'
      when k in ('CH9','KM09','GH9') then 'CH9'
      when k in ('HD1','KMHD1') then 'HD1'
      when k in ('BN1','KMBN1') then 'BN1'
      when k in ('TMDT','KMTMDT') then 'TMDT'
      else k
    end as ma_kho_map
  from unnest([
    struct('CH1' as k)
  ])
),

-- 3.1 DT (Doanh thu)
dt_base as (
  select
    date(f.Ngay_PhieuThu) as Ngay,
    cast(f.Ma_Cong_Ty as string) as ma_cong_ty,
    cast(f.Ma_Kho as string) as kenh_ban,

    cast(dmnh.Ma_Nhom as string) as nhom_hang,

    safe_cast(dmnmm.ID as int64) as ID_ma_mau,
    cast(dmnmm.Ma_NM as string) as ma_mau,
    cast(dmnmm.Mo_Ta as string) as mo_ta_ma_mau,

    cast(bost.Ten as string) as bo_suu_tap,
    cast(chungl.Ten as string) as chung_loai,
    cast(nhomct.Ten as string) as danh_muc_san_pham,
    cast(hoatiet.Ten as string) as hoa_tiet_mat,
    cast(gioit.Ten as string) as gioi_tinh_san_pham,
    cast(tendc.Ten as string) as ten_da_chu,
    cast(loaida.Ten as string) as loai_da,
    cast(hamlkl.Ten as string) as ham_luong_kim_loai,

    dmnmm.ngay_tao_ma_mau,

    coalesce(safe_cast(f.Tien_PhieuThu1 as bignumeric), 0) as Tien_PhieuThu1,
    coalesce(safe_cast(f.Gia_Von as bignumeric), 0) as Gia_Von,
    coalesce(safe_cast(f.So_Luong as bignumeric), 0) as So_Luong,
    coalesce(safe_cast(f.SL_Thuc_Te as bignumeric), 0) as SL_Thuc_Te,

    cast(f.Ma_KH as string) as Ma_KH,
    safe_cast(f.ID_Phieu_thu as int64) as ID_Phieu_thu

  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_doanh_thu` f
  cross join params p
  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` dmh
    on safe_cast(f.ID_Hang as int64) = safe_cast(dmh.ID as int64)
  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` dmnh
    on safe_cast(dmh.ID_Nhom as int64) = safe_cast(dmnh.ID as int64)
  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom_ma_mau` dmnmm
    on safe_cast(dmh.ID_NMM as int64) = safe_cast(dmnmm.ID as int64)

  left join nhomct
    on safe_cast(dmnmm.ID_NhomCt as int64) = safe_cast(nhomct.ID_Stt as int64)
  left join chungl
    on safe_cast(dmnmm.ID_ChungL as int64) = safe_cast(chungl.ID_Stt as int64)
  left join gioit
    on safe_cast(dmnmm.ID_GioiT as int64) = safe_cast(gioit.ID_Stt as int64)
  left join hoatiet
    on safe_cast(dmnmm.ID_HoaTiet as int64) = safe_cast(hoatiet.ID_Stt as int64)
  left join bost
    on safe_cast(dmnmm.ID_BoST as int64) = safe_cast(bost.ID_Stt as int64)
  left join loaida
    on safe_cast(dmnmm.ID_LoaiDa as int64) = safe_cast(loaida.ID_Stt as int64)
  left join tendc
    on safe_cast(dmnmm.ID_TenDC as int64) = safe_cast(tendc.ID_Stt as int64)
  left join hamlkl
    on safe_cast(dmnmm.ID_HamLKL as int64) = safe_cast(hamlkl.ID_Stt as int64)

  where 1=1
    and Ma_dong is null
    
    and date(f.Ngay_PhieuThu) >= p.backfill_start_date
    and date(f.Ngay_PhieuThu) <= p.report_date
    
),

dt as (
  select
    Ngay,
    ma_cong_ty,
    kenh_ban,
    nhom_hang,
    ID_ma_mau,
    ma_mau,
    mo_ta_ma_mau,
    bo_suu_tap,
    chung_loai,
    danh_muc_san_pham,
    hoa_tiet_mat,
    gioi_tinh_san_pham,
    ten_da_chu,
    loai_da,
    ham_luong_kim_loai,
    ngay_tao_ma_mau,

    sum(Tien_PhieuThu1) as doanh_thu_thuan,
    sum(Gia_Von) as tong_tien_von,
    sum(Tien_PhieuThu1 - Gia_Von) as loi_nhuan_gop,

    sum(So_Luong) as so_luong_ban,
    sum(SL_Thuc_Te) as so_luong_chi,

    count(distinct Ma_KH) as so_luong_khach_mua,
    count(distinct ID_Phieu_thu) as so_luong_don_hang

  from dt_base
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
),

-- 3.2 HTK (tồn kho ngày) 
htk as (
  select
    date(tk.ngay) as HTK_Ngay,
    cast(tk.nguon as string) as HTK_Nguon,
    (
      case
        when cast(tk.ma_kho as string) in ('CH1','KM01','GH1') then 'CH1'
        when cast(tk.ma_kho as string) in ('CH2','KM02','GH2') then 'CH2'
        when cast(tk.ma_kho as string) in ('CH3','KM03','GH3') then 'CH3'
        when cast(tk.ma_kho as string) in ('CH4','KM04','GH4') then 'CH4'
        when cast(tk.ma_kho as string) in ('CH5','KM05','GH5') then 'CH5'
        when cast(tk.ma_kho as string) in ('CH6','KM06','GH6') then 'CH6'
        when cast(tk.ma_kho as string) in ('CH7','KM07','GH7') then 'CH7'
        when cast(tk.ma_kho as string) in ('CH8','KM08','GH8') then 'CH8'
        when cast(tk.ma_kho as string) in ('CH9','KM09','GH9') then 'CH9'
        when cast(tk.ma_kho as string) in ('HD1','KMHD1') then 'HD1'
        when cast(tk.ma_kho as string) in ('BN1','KMBN1') then 'BN1'
        when cast(tk.ma_kho as string) in ('TMDT','KMTMDT') then 'TMDT'
        else cast(tk.ma_kho as string)
      end
    ) as HTK_Ma_Kho,

    safe_cast(h.ID_NMM as int64) as HTK_ID_NMM,

    sum(coalesce(safe_cast(tk.so_luong_ton as bignumeric), 0)) as Sl_Ton,
    sum(
      case
        when date(tk.ngay_nhap) = date(tk.ngay) then coalesce(safe_cast(tk.so_luong_ton as bignumeric), 0)
        else 0
      end
    ) as sl_nhap

  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_ton_kho` tk
  cross join params p
  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` h
    on safe_cast(tk.id_hang as int64) = safe_cast(h.ID as int64)

  where 1=1
    
    and date(tk.ngay) >= p.backfill_start_date
    and date(tk.ngay) <= p.report_date
    
  group by 1,2,3,4
),

-- 3.3 HML (hàng mua lại) (removed)

-- 3.4 NX
nx as (
  select
    date(f.Ngay) as NX_Ngay,
    cast(f.Nguon as string) as NX_Nguon,
    (
      case
        when cast(f.Ma_Kho as string) in ('CH1','KM01','GH1') then 'CH1'
        when cast(f.Ma_Kho as string) in ('CH2','KM02','GH2') then 'CH2'
        when cast(f.Ma_Kho as string) in ('CH3','KM03','GH3') then 'CH3'
        when cast(f.Ma_Kho as string) in ('CH4','KM04','GH4') then 'CH4'
        when cast(f.Ma_Kho as string) in ('CH5','KM05','GH5') then 'CH5'
        when cast(f.Ma_Kho as string) in ('CH6','KM06','GH6') then 'CH6'
        when cast(f.Ma_Kho as string) in ('CH7','KM07','GH7') then 'CH7'
        when cast(f.Ma_Kho as string) in ('CH8','KM08','GH8') then 'CH8'
        when cast(f.Ma_Kho as string) in ('CH9','KM09','GH9') then 'CH9'
        when cast(f.Ma_Kho as string) in ('HD1','KMHD1') then 'HD1'
        when cast(f.Ma_Kho as string) in ('BN1','KMBN1') then 'BN1'
        when cast(f.Ma_Kho as string) in ('TMDT','KMTMDT') then 'TMDT'
        else cast(f.Ma_Kho as string)
      end
    ) as NX_Ma_Kho,

    safe_cast(h.ID_NMM as int64) as NX_ID_NMM,

    sum(coalesce(safe_cast(f.So_Luong_Theo_Dvt as bignumeric), 0)) as NX_So_Luong

  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_nhap_xuat` f
  cross join params p
  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hach_toan` dm
    on safe_cast(f.ID_Nx as int64) = safe_cast(dm.ID as int64)
    and cast(f.Nguon as string) = cast(dm.Nguon as string)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` h
    on safe_cast(f.ID_Hang as int64) = safe_cast(h.ID as int64)

  where 1=1
    
    and date(f.Ngay) >= p.backfill_start_date
    and date(f.Ngay) <= p.report_date
    
    and cast(dm.Ma_Ct as string) in ('NK','NM','NL','NS','PN')

  group by 1,2,3,4
),

final_source as (
  select
    dt.*,

    htk.Sl_Ton,
    htk.sl_nhap,

    cast(0 as bignumeric) as HML_So_luong,
    nx.NX_So_Luong

  from dt
  left join htk
    on dt.Ngay = htk.HTK_Ngay
    and dt.ma_cong_ty = htk.HTK_Nguon
    and dt.kenh_ban = htk.HTK_Ma_Kho
    and dt.ID_ma_mau = htk.HTK_ID_NMM

  left join nx
    on dt.Ngay = nx.NX_Ngay
    and dt.ma_cong_ty = nx.NX_Nguon
    and dt.kenh_ban = nx.NX_Ma_Kho
    and dt.ID_ma_mau = nx.NX_ID_NMM
)

select
  Ngay,
  ma_cong_ty as MA_CONG_TY,
  kenh_ban as KENH_BAN,
  nhom_hang as NHOM_HANG,
  ma_mau as MA_MAU,
  mo_ta_ma_mau as MO_TA_MA_MAU,
  bo_suu_tap as BO_SUU_TAP,
  chung_loai as CHUNG_LOAI,
  danh_muc_san_pham as DANH_MUC_SAN_PHAM,
  hoa_tiet_mat as HOA_TIET_MAT,
  gioi_tinh_san_pham as GIOI_TINH_SAN_PHAM,
  ten_da_chu as TEN_DA_CHU,
  loai_da as LOAI_DA,
  ham_luong_kim_loai as HAM_LUONG_KIM_LOAI,
  ngay_tao_ma_mau as NGAY_TAO_MA_MAU,

  cast(round(coalesce(doanh_thu_thuan, 0), 2) as numeric) as DOANH_THU_THUAN,
  cast(round(coalesce(tong_tien_von, 0), 2) as numeric) as TONG_TIEN_VON,
  cast(round(coalesce(loi_nhuan_gop, 0), 2) as numeric) as LOI_NHUAN_GOP,

  cast(coalesce(so_luong_ban, 0) as int64) as SO_LUONG_BAN,
  cast(coalesce(so_luong_chi, 0) as int64) as SO_LUONG_CHI,

  cast(coalesce(NX_So_Luong, 0) as int64) + cast(coalesce(HML_So_luong, 0) as int64) as SO_LUONG_NHAP,

  cast(coalesce(so_luong_khach_mua, 0) as int64) as SO_LUONG_KHACH_MUA,
  cast(coalesce(Sl_Ton, 0) as int64) as SO_LUONG_TON_KHO,
  cast(coalesce(so_luong_don_hang, 0) as int64) as SO_LUONG_DON_HANG,
  cast(coalesce(HML_So_luong, 0) as int64) as SO_LUONG_MUA_LAI,

  current_timestamp() as UpdateTime

from final_source

