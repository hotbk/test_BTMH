



with params as (
  select
    
(
  
    
(
  
    current_date()
  
)

  
)
 as run_date,
    
(
  
    
(
  
    date_sub(
(
  
    current_date()
  
)
, interval 6 month)
  
)

  
)
 as start_date,
    date_trunc(
(
  
    
(
  
    date_sub(
(
  
    current_date()
  
)
, interval 6 month)
  
)

  
)
, month) as start_month,
    date_add(date_trunc(
(
  
    
(
  
    current_date()
  
)

  
)
, month), interval 1 month) as end_excl
),

nx as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_nhap_xuat`
),

ht as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hach_toan`
),

hang as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang`
),

nmm as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom_ma_mau`
),

vbmm as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmvbmm`
  where cDM = 'NHOMCT'
),

dt as (
  select *
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_doi_tac`
),

joined as (
  select
    nx.*,
    ht.Ma_Ct,

    hang.Ma_Hang,
    hang.Ten_Hang,
    hang.Ma_Nhom,
    hang.ID_NMM,

    nmm.ID_NhomCt,

    vbmm.Ma as Ma_Nhom_Ct,
    vbmm.Ten as Ten_Nhom_Ct,
    dt.Ten_Dt,
    dt.Dia_chi,
    dt.Dien_Thoai,
    dt.Ngay_Sinh,
    dt.So_CMT

  from nx
  left join ht
    on safe_cast(nx.ID_Nx as int64) = safe_cast(ht.ID as int64)
    and cast(nx.Nguon as string) = cast(ht.Nguon as string)

  left join hang
    on safe_cast(nx.ID_Hang as int64) = safe_cast(hang.ID as int64)

  left join nmm
    on safe_cast(hang.ID_NMM as int64) = safe_cast(nmm.ID as int64)

  left join vbmm
    on safe_cast(nmm.ID_NhomCt as int64) = safe_cast(vbmm.ID_Stt as int64)

  left join dt
    on cast(nx.Ma_Dt as string) = cast(dt.Ma_Dt as string)
),

b2b as (
  select *
  from joined
  where Ma_Ct = 'XB'
    and Ma_Nhom_Ct in (
      'TT', 'PY', '24', 'VD', 'VT', 'PH', 'NC', 'TK', 'KC', 'BA',
      'BX', 'PT', 'CN', '23', 'NH', 'BY', 'BC', 'BT'
    )
    and Sub_ID is null
)

select
  -- 1. ĐỊNH DANH
  cast(ID as string) as ID_Phieu_Ban,
  safe_cast(ID_Nx as int64) as ID_Nx,
  cast(Ma_HT as string) as Loai_Chung_Tu,
  date(Ngay) as Ngay_Chung_Tu,
  cast(Sp as string) as So_Phieu,

  -- 2. KHÁCH HÀNG
  cast(Ma_Dt as string) as Ma_Khach_Hang,
  cast(Ten_Dt as string) as Ten_Khach_Hang,
  concat("'", cast(Dien_Thoai as string)) as Dien_Thoai,
  cast(Dia_chi as string) as Dia_Chi,
  format_date('%Y-%m-%d', date(Ngay_Sinh)) as Ngay_Sinh,
  cast(So_CMT as string) as CCCD_CMT,

  -- 3. KHO & TÀI KHOẢN
  cast(Ma_Kho as string) as Ma_Kho,
  cast(No_Tk as string) as Tai_Khoan_No,
  cast(Co_Tk as string) as Tai_Khoan_Co,

  -- 4. SẢN PHẨM
  safe_cast(ID_Hang as int64) as ID_Hang,
  cast(Ma_Hang as string) as Ma_Hang,
  cast(Ten_Hang as string) as Ten_Hang,
  cast(Ma_Nhom as string) as Nhom_Hang,
  cast(Ma_Nhom_Ct as string) as Ma_Nhom_Ct,
  cast(Ten_Nhom_Ct as string) as Ten_Nhom_Ct,

  -- 5. SỐ LƯỢNG / TRỌNG LƯỢNG
  safe_cast(So_Luong_Theo_Dvt as bignumeric) as So_Luong,
  safe_cast(SL_Chi_TT as bignumeric) as SL_Chi_TT,
  safe_cast(Tong_tlg as bignumeric) as Tong_Trong_Luong,

  -- 6. TÀI CHÍNH
  safe_cast(Don_Gia as bignumeric) as Don_Gia_Ban,
  safe_cast(T_Tien as bignumeric) as Doanh_Thu_Ban,
  safe_cast(Don_Gia1 as bignumeric) as Don_Gia_Von_Theo_SP,
  safe_cast(T_Tien1 as bignumeric) as Tong_Gia_Von,
  (safe_cast(T_Tien as bignumeric) - safe_cast(T_Tien1 as bignumeric)) as Loi_Nhuan_Gop,

  -- 7. THUẾ
  safe_cast(Gia_VAT as bignumeric) as Thue_VAT,
  safe_cast(Gia_KVat as bignumeric) as Tien_Truoc_Thue,

  current_timestamp() as UpdateTime

from b2b
cross join params p

