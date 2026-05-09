



with params as (
  select
    
(
  
    current_date()
  
)
 as run_date,
    
(
  
    date_sub(
(
  
    current_date()
  
)
, interval 4 month)
  
)
 as start_date,
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

base as (
  select
    cast(f.Ma_KH as string) as ma_dt,
    cast(f.ID as string) as ID,
    cast(f.ID_Phieu_thu as string) as ID_Phieu_thu,
    cast(f.Ngay_PhieuThu as datetime) as ngay_phieuthu,

    safe_cast(f.ID_Dv as int64) as ID_Dv,

    safe_cast(f.ID_Hang as int64) as ID_Hang,
    cast(f.Ma_Kho as string) as Ma_Kho,

    coalesce(safe_cast(f.So_Luong as bignumeric), 0) as So_Luong,
    coalesce(safe_cast(f.SL_Chi_TT as bignumeric), 0) as SL_Chi_TT,
    coalesce(safe_cast(f.Tien_PhieuThu1 as bignumeric), 0) as Tien_PhieuThu1,
    coalesce(safe_cast(f.Gia_Von as bignumeric), 0) as Gia_Von,
    coalesce(safe_cast(f.Don_gia_ban as bignumeric), 0) as Don_gia_ban,
    coalesce(safe_cast(f.Tien_Chiet_Khau as bignumeric), 0) as Tien_Chiet_Khau,

    cast(f.Ma_Cong_Ty as string) as Ma_Cong_Ty

  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_doanh_thu` f
  cross join params p
  where 1=1
  and Ma_dong is null
    
    and date(f.Ngay_PhieuThu) >= p.backfill_start_date
    and date(f.Ngay_PhieuThu) < date_add(date(p.run_date), interval 1 day)
    
),

ranked_orders as (
  select
    b.ma_dt,
    b.ID,
    b.ID_Phieu_thu,
    b.ngay_phieuthu,
    b.ID_Hang,

    cast(dmnh.Ma_Nhom as string) as Ma_Nhom,

    lag(b.ngay_phieuthu) over (
      partition by b.ma_dt
      order by b.ngay_phieuthu
    ) as PreviousOrderDate,

    case
      when cast(dmnh.Ma_Nhom as string) like 'NL24%'
        or cast(dmnh.Ma_Nhom as string) like 'NLTT%'
        or cast(dmnh.Ma_Nhom as string) like 'KTD%'
        or cast(dmnh.Ma_Nhom as string) like 'KHS%'
        or cast(dmnh.Ma_Nhom as string) like 'KGB%'
        or cast(dmnh.Ma_Nhom as string) like 'TTS%'
        or cast(dmnh.Ma_Nhom as string) like 'TTV%'
        or cast(dmnh.Ma_Nhom as string) like 'TT-24%'
        or cast(dmnh.Ma_Nhom as string) like 'TTM%'
        or cast(dmnh.Ma_Nhom as string) like 'TTN%'
        or cast(dmnh.Ma_Nhom as string) like 'TTX%'
        then 0
      else 1
    end as categorize_product

  from base b
  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` dmh
    on safe_cast(dmh.ID as int64) = b.ID_Hang
  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` dmnh
    on safe_cast(dmnh.ID as int64) = safe_cast(dmh.ID_Nhom as int64)
),

distinct_order_dates as (
  select distinct
    b.ma_dt,
    date(b.ngay_phieuthu) as ngay_phieuthu
  from base b
),

previous_order_date as (
  select
    ro.ma_dt,
    ro.ID,
    ro.ngay_phieuthu,
    max(dod.ngay_phieuthu) as LastOrderDateBeforeCurrent
  from ranked_orders ro
  left join distinct_order_dates dod
    on ro.ma_dt = dod.ma_dt
    and dod.ngay_phieuthu < date(ro.ngay_phieuthu)
  group by ro.ma_dt, ro.ID, ro.ngay_phieuthu
),

customer_type as (
  select
    ma_dt,
    ID,
    ngay_phieuthu,
    case
      when LastOrderDateBeforeCurrent is null then 'New'
      when date_diff(date(ngay_phieuthu), LastOrderDateBeforeCurrent, day) <= 365 then 'Returning'
      else 'New'
    end as CustomerType
  from previous_order_date
),

receipt_purchase_type as (
  select
    ID_Phieu_thu,
    case
      when sum(case when categorize_product = 0 then 1 else 0 end) > 0
        and sum(case when categorize_product = 1 then 1 else 0 end) > 0 then 'Mua cả hai'
      when sum(case when categorize_product = 0 then 1 else 0 end) > 0 then 'Chỉ mua tích trữ'
      when sum(case when categorize_product = 1 then 1 else 0 end) > 0 then 'Chỉ mua trang sức'
    end as ReceiptPurchaseType
  from ranked_orders
  group by ID_Phieu_thu
),

final as (
  select
    cast(b.ngay_phieuthu as datetime) as NGAY,
    cast(b.Ma_Cong_Ty as string) as MA_CONG_TY,
    cast(b.Ma_Cong_Ty as string) as `MA CONG TY`,
    safe_cast(b.ID_Dv as int64) as ID_Dv,
    cast(b.ID as string) as `ID`,
    cast(b.ID_Phieu_thu as string) as `ID PT`,

    cast(dmh.Ma_Hang as string) as `MA HANG`,

    cast(dmnmm.Ma_NM as string) as `MA MAU`,
    cast(dmnmm.Mo_Ta as string) as `MO TA MA MAU`,
    cast(dmnmm.Anh_NM as string) as `ANH MA MAU`,

    cast(b.Ma_Kho as string) as KENH_BAN,
    cast(b.Ma_Kho as string) as `KENH BAN`,
    cast(dmnh.Ma_Nhom as string) as NHOM_HANG,
    cast(dmnh.Ma_Nhom as string) as `NHOM HANG`,

    cast(bost.Ten as string) as `BO SUU TAP`,
    cast(chungl.Ten as string) as `CHUNG LOAI`,
    cast(nhomct.Ten as string) as `DANH MUC SAN PHAM`,
    cast(hoatiet.Ten as string) as `HOA TIET MAT`,
    cast(gioit.Ten as string) as `GIOI TINH SAN PHAM`,
    cast(tendc.Ten as string) as `TEN DA CHU`,
    cast(loaida.Ten as string) as `LOAI DA`,
    cast(hamlkl.Ten as string) as `HAM LUONG KIM LOAI`,

    safe_cast(dmh.T_Luong as bignumeric) as `TRONG LUONG VANG`,
    safe_cast(dmh.The_Tich as bignumeric) as `TRONG LUONG DA CHINH`,
    safe_cast(dmh.Tien_Lai as bignumeric) as `TRONG LUONG DA PHU`,
    (
      coalesce(safe_cast(dmh.T_Luong as bignumeric), 0)
      + coalesce(safe_cast(dmh.The_Tich as bignumeric), 0)
      + coalesce(safe_cast(dmh.Tien_Lai as bignumeric), 0)
      + coalesce(safe_cast(dmh.Tyle_Lai as bignumeric), 0)
    ) as `TONG TRONG LUONG`,

    cast(dmh.Ma_NCC as string) as `MA NCC`,

    cast(b.ma_dt as string) as `MA KH`,
    cast(dtdt.Ten_Dt as string) as `TEN KH`,
    cast(dtdt.Gioi_Tinh as string) as `GIOI TINH`,

    (
      case
        when cast(b.Ma_Kho as string) = 'TMDT' and date(dtdt.Ngay_Sinh) = date('1990-01-01') then 999
        when dtdt.Ngay_Sinh is null then null
        else date_diff(date(b.ngay_phieuthu), date(dtdt.Ngay_Sinh), year)
      end
    ) as TUOI,

    cast(date(dtdt.Ngay_Sinh) as date) as `NGAY SINH`,

    cast(ct.CustomerType as string) as LOAI_KH,
    cast(ct.CustomerType as string) as `LOAI KH`,
    cast(rpt.ReceiptPurchaseType as string) as LOAI_PHIEU_THU,
    cast(rpt.ReceiptPurchaseType as string) as `LOAI PHIEU THU`,

    safe_cast(b.So_Luong as bignumeric) as `SO LUONG`,
    safe_cast(b.SL_Chi_TT as bignumeric) as `SO LUONG CHI`,
    safe_cast(b.Tien_PhieuThu1 as bignumeric) as `DOANH THU THUAN`,
    safe_cast(b.Gia_Von as bignumeric) as `GIA VON`,
    safe_cast(b.Don_gia_ban as bignumeric) as `DON GIA BAN`,
    safe_cast(b.Tien_Chiet_Khau as bignumeric) as `CHIET KHAU`,

    current_timestamp() as UpdateTime

  from base b
  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` dmh
    on safe_cast(dmh.ID as int64) = b.ID_Hang
  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` dmnh
    on safe_cast(dmnh.ID as int64) = safe_cast(dmh.ID_Nhom as int64)
  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom_ma_mau` dmnmm
    on safe_cast(dmnmm.ID as int64) = safe_cast(dmh.ID_NMM as int64)

  left join bost
    on safe_cast(dmnmm.ID_BoST as int64) = safe_cast(bost.ID_Stt as int64)
  left join chungl
    on safe_cast(dmnmm.ID_ChungL as int64) = safe_cast(chungl.ID_Stt as int64)
  left join nhomct
    on safe_cast(dmnmm.ID_NhomCt as int64) = safe_cast(nhomct.ID_Stt as int64)
  left join hoatiet
    on safe_cast(dmnmm.ID_HoaTiet as int64) = safe_cast(hoatiet.ID_Stt as int64)
  left join gioit
    on safe_cast(dmnmm.ID_GioiT as int64) = safe_cast(gioit.ID_Stt as int64)
  left join tendc
    on safe_cast(dmnmm.ID_TenDC as int64) = safe_cast(tendc.ID_Stt as int64)
  left join loaida
    on safe_cast(dmnmm.ID_LoaiDa as int64) = safe_cast(loaida.ID_Stt as int64)
  left join hamlkl
    on safe_cast(dmnmm.ID_HamLKL as int64) = safe_cast(hamlkl.ID_Stt as int64)

  left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_doi_tac` dtdt
    on cast(dtdt.Ma_Dt as string) = cast(b.ma_dt as string)

  left join customer_type ct
    on ct.ID = b.ID

  left join receipt_purchase_type rpt
    on cast(rpt.ID_Phieu_thu as string) = cast(b.ID_Phieu_thu as string)
)

select * from final