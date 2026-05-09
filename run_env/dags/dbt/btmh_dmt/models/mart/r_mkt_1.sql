{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='sync_all_columns',
    partition_by={"field": "Ngay", "data_type": "datetime"},
    cluster_by=['Ngay', 'ID']
  )
}}

{% set src_schema = (var('d_hang_schema', 'stg_augges_225') | lower) %}

with params as (
  select
    {{ btmh_run_date() }} as run_date,
    {{ btmh_start_date(4) }} as start_date
),

vbmm as (
  select *
  from {{ source(src_schema, 'dmvbmm') }}
),

nhomct as (
  select ID_Stt, Ten
  from vbmm
  where cast(cDM as string) = 'NHOMCT'
),
chungl as (
  select ID_Stt, Ten
  from vbmm
  where cast(cDM as string) = 'CHUNGL'
),
gioit as (
  select ID_Stt, Ten
  from vbmm
  where cast(cDM as string) = 'GIOIT'
),
hoatiet as (
  select ID_Stt, Ten
  from vbmm
  where cast(cDM as string) = 'HOATIET'
),
bost as (
  select ID_Stt, Ten
  from vbmm
  where cast(cDM as string) = 'BOST'
),
tendc as (
  select ID_Stt, Ten
  from vbmm
  where cast(cDM as string) = 'TENDC'
),
hamlkl as (
  select ID_Stt, Ten
  from vbmm
  where cast(cDM as string) = 'HAMLKL'
),

f_base as (
  select
    cast(f.Ma_Cong_Ty as string) as Ma_Cong_Ty,
    cast(f.Ma_Dt as string) as Ma_Dt,
    cast(f.Ma_KH as string) as Ma_KH,

    cast(f.ID as string) as ID,
    cast(safe_cast(f.ID_Phieu_thu as int64) as string) as ID_Phieu_thu,
    cast(f.Ngay_PhieuThu as datetime) as Ngay_PhieuThu,

    cast(f.ID_Hang as string) as ID_Hang,
    cast(f.Ma_Kho as string) as Ma_Kho,
    cast(f.Ma_Nv as string) as Ma_Nv,

    safe_cast(f.So_Luong as bignumeric) as So_Luong,
    safe_cast(f.SL_Chi_TT as bignumeric) as SL_Chi_TT,

    safe_cast(f.Tien_PhieuThu1 as bignumeric) as Tien_PhieuThu1,
    safe_cast(f.Gia_Von as bignumeric) as Gia_Von,
    safe_cast(f.Don_gia_ban as bignumeric) as Don_gia_ban,
    safe_cast(f.Tien_Chiet_Khau as bignumeric) as Tien_Chiet_Khau,
    safe_cast(f.Tien_giam_gia_TP as bignumeric) as Tien_giam_gia_TP,

    cast(f.ctkm_kh as string) as ctkm_kh,
    cast(f.ctkm_nd as string) as ctkm_nd,
    cast(f.ctkm_kh_pt as string) as ctkm_kh_pt,
    cast(f.ctkm_nd_pt as string) as ctkm_nd_pt,

    cast(f.so_the_voucher as string) as so_the_voucher,
    cast(f.ten_the_voucher as string) as ten_the_voucher

  from {{ ref('f_doanh_thu') }} f
  cross join params p
  where safe_cast(f.Thanh_tien_theo_DG_ban as bignumeric) > 0
    and safe_cast(f.So_Luong as bignumeric) > 0
    and safe_cast(f.ID_Dv as int64) = 0

  {% if is_incremental() %}
    and date(f.Ngay_PhieuThu) >= date(p.start_date)
    and date(f.Ngay_PhieuThu) < date_add(date(p.run_date), interval 1 day)
  {% endif %}
),

h as (select * from {{ ref('d_hang') }}),
nh as (select * from {{ ref('d_nhom') }}),
nmm as (select * from {{ ref('d_nhom_ma_mau') }}),
dt as (select * from {{ ref('d_doi_tac') }}),
nv as (select * from {{ ref('d_nhan_vien') }}),

ranked_orders as (
  -- Line-level rows for classification.
  select
    f.Ma_Cong_Ty,
    f.Ma_Dt,
    f.ID,
    f.ID_Phieu_thu,
    f.Ngay_PhieuThu,
    cast(h.Ma_Hang as string) as Ma_Hang,
    cast(nh.Ma_Nhom as string) as Ma_Nhom,
    case
      when cast(nh.Ma_Nhom as string) like 'NL24%'
        or cast(nh.Ma_Nhom as string) like 'NLTT%'
        or cast(nh.Ma_Nhom as string) like 'KHS%'
        or cast(nh.Ma_Nhom as string) like 'KGB%'
        or cast(nh.Ma_Nhom as string) like 'TTS%'
        or cast(nh.Ma_Nhom as string) like 'TTV%'
        then 0
      else 1
    end as categorize_product
  from f_base f
  left join h
    on safe_cast(h.ID as int64) = safe_cast(f.ID_Hang as int64)
  left join nh
    on safe_cast(nh.ID as int64) = safe_cast(h.ID_Nhom as int64)
),

order_dates as (
  -- Distinct purchase days per customer, with the previous distinct day.
  select
    Ma_Cong_Ty,
    Ma_Dt,
    ngay_phieuthu,
    lag(ngay_phieuthu) over (
      partition by Ma_Cong_Ty, Ma_Dt
      order by ngay_phieuthu
    ) as last_order_date_before_current
  from (
    select distinct
      Ma_Cong_Ty,
      Ma_Dt,
      date(Ngay_PhieuThu) as ngay_phieuthu
    from f_base
  )
),

customer_type as (
  select
    ro.Ma_Cong_Ty,
    ro.Ma_Dt,
    ro.ID,
    ro.Ngay_PhieuThu,
    case
      when od.last_order_date_before_current is null then 'New'
      when date_diff(date(ro.Ngay_PhieuThu), od.last_order_date_before_current, day) <= 365 then 'Returning'
      else 'New'
    end as CustomerType
  from ranked_orders ro
  left join order_dates od
    on ro.Ma_Cong_Ty = od.Ma_Cong_Ty
    and ro.Ma_Dt = od.Ma_Dt
    and date(ro.Ngay_PhieuThu) = od.ngay_phieuthu
),

receipt_purchase_type as (
  select
    Ma_Cong_Ty,
    ID_Phieu_thu,
    date(Ngay_PhieuThu) as Ngay_PhieuThu,
    Ma_Dt,
    case
      when sum(case when categorize_product = 0 then 1 else 0 end) > 0
        and sum(case when categorize_product = 1 then 1 else 0 end) > 0
        then 'Mua cả hai'
      when sum(case when categorize_product = 0 then 1 else 0 end) > 0
        then 'Chỉ mua tích trữ'
      when sum(case when categorize_product = 1 then 1 else 0 end) > 0
        then 'Chỉ mua trang sức'
    end as ReceiptPurchaseType
  from ranked_orders
  group by Ma_Cong_Ty, ID_Phieu_thu, Ngay_PhieuThu, Ma_Dt
),

customer_purchase_type as (
  -- Transaction type for this receipt based on customer's receipts in the last 365 days.
  select
    r0.Ma_Cong_Ty,
    r0.ID_Phieu_thu,
    case
      when sum(case when r1.ReceiptPurchaseType = 'Mua cả hai' then 1 else 0 end) > 0
        or (
          sum(case when r1.ReceiptPurchaseType = 'Chỉ mua tích trữ' then 1 else 0 end) > 0
          and sum(case when r1.ReceiptPurchaseType = 'Chỉ mua trang sức' then 1 else 0 end) > 0
        )
        then 'Mua cả hai'
      when sum(case when r1.ReceiptPurchaseType = 'Chỉ mua tích trữ' then 1 else 0 end) > 0
        then 'Chỉ mua tích trữ'
      when sum(case when r1.ReceiptPurchaseType = 'Chỉ mua trang sức' then 1 else 0 end) > 0
        then 'Chỉ mua trang sức'
    end as CustomerTransactionType
  from receipt_purchase_type r0
  left join receipt_purchase_type r1
    on r1.Ma_Cong_Ty = r0.Ma_Cong_Ty
    and r1.Ma_Dt = r0.Ma_Dt
    and r1.Ngay_PhieuThu >= date_sub(r0.Ngay_PhieuThu, interval 365 day)
    and r1.Ngay_PhieuThu <= r0.Ngay_PhieuThu
  group by r0.Ma_Cong_Ty, r0.ID_Phieu_thu
)

select
  cast(f.Ngay_PhieuThu as datetime) as Ngay,
  cast(f.Ma_Cong_Ty as string) as `MA CONG TY`,
  cast(f.ID as string) as ID,
  cast(f.ID_Phieu_thu as string) as `ID PT`,

  cast(h.Ma_Hang as string) as `MA HANG`,
  cast(nmm.Ma_NM as string) as `MA MAU`,
  cast(nmm.Mo_Ta as string) as `MO TA MA MAU`,
  cast(nmm.Anh_NM as string) as `ANH MA MAU`,

  cast(f.Ma_Kho as string) as `KENH BAN`,
  cast(nh.Ma_Nhom as string) as `NHOM HANG`,
  cast(h.dong_san_pham as string) as `DONG SAN PHAM`,

  cast(bost.Ten as string) as `BO SUU TAP`,
  cast(chungl.Ten as string) as `CHUNG LOAI`,
  cast(nhomct.Ten as string) as `DANH MUC SAN PHAM`,
  cast(hoatiet.Ten as string) as `HOA TIET MAT`,
  cast(gioit.Ten as string) as `GIOI TINH SAN PHAM`,
  cast(tendc.Ten as string) as `TEN DA CHU`,
  cast(hamlkl.Ten as string) as `HAM LUONG KIM LOAI`,

  safe_cast(h.T_Luong as bignumeric) as `TRONG LUONG VANG`,
  (
    coalesce(safe_cast(h.T_Luong as bignumeric), 0)
    + coalesce(safe_cast(h.The_Tich as bignumeric), 0)
    + coalesce(safe_cast(h.Tien_Lai as bignumeric), 0)
    + coalesce(safe_cast(h.Tyle_Lai as bignumeric), 0)
  ) as `TONG TRONG LUONG`,

  cast(f.Ma_KH as string) as `MA KH`,
  cast(dt.Dien_Thoai as string) as `DIEN THOAI`,
  cast(dt.Gioi_Tinh as string) as `GIOI TINH`,

  (
    case
      when cast(f.Ma_Kho as string) = 'TMDT' and date(dt.Ngay_Sinh) = date '1990-01-01' then 999
      else date_diff(date(f.Ngay_PhieuThu), date(dt.Ngay_Sinh), year)
    end
  ) as TUOI,

  date(dt.Ngay_Sinh) as `NGAY SINH`,

  cast(ct.CustomerType as string) as `LOAI KH`,
  cast(dt.Tinh as string) as Tinh,
  cast(dt.Quan as string) as Quan,
  cast(dt.Phuong as string) as Phuong,

  cast(rpt.ReceiptPurchaseType as string) as `LOAI PHIEU THU`,
  cast(cpt.CustomerTransactionType as string) as `LOAI PHIEU THU 2`,

  safe_cast(f.So_Luong as bignumeric) as `SO LUONG`,
  safe_cast(f.SL_Chi_TT as bignumeric) as `SO LUONG CHI`,
  safe_cast(f.Tien_PhieuThu1 as bignumeric) as `DOANH THU THUAN`,
  safe_cast(f.Gia_Von as bignumeric) as `GIA VON`,
  safe_cast(f.Don_gia_ban as bignumeric) as `DON GIA BAN`,
  safe_cast(f.Tien_Chiet_Khau as bignumeric) as `CHIET KHAU`,

  cast(f.ctkm_kh as string) as `MA CTKM`,
  cast(f.ctkm_nd as string) as `ND CTKM`,
  cast(f.ctkm_kh_pt as string) as `MA CTKM PT`,
  cast(f.ctkm_nd_pt as string) as `ND CTKM PT`,

  cast(f.so_the_voucher as string) as `SO THE VOUCHER`,
  cast(f.ten_the_voucher as string) as `TEN THE VOUCHER`,
  safe_cast(f.Tien_giam_gia_TP as bignumeric) as `TIEN GIAM`,

  cast(nv.Ma_Nv as string) as Ma_Nv,
  cast(nv.Ten_Nv as string) as Ten_Nv,

  current_timestamp() as UpdateTime

from f_base f
left join h
  on safe_cast(h.ID as int64) = safe_cast(f.ID_Hang as int64)
left join nmm
  on safe_cast(nmm.ID as int64) = safe_cast(h.ID_NMM as int64)
left join nh
  on safe_cast(nh.ID as int64) = safe_cast(h.ID_Nhom as int64)
left join nv
  on cast(nv.Ma_Nv as string) = cast(f.Ma_Nv as string)
left join dt
  on cast(dt.Ma_Dt as string) = cast(f.Ma_Dt as string)

left join nhomct
  on safe_cast(nmm.ID_NhomCt as int64) = safe_cast(nhomct.ID_Stt as int64)
left join chungl
  on safe_cast(nmm.ID_ChungL as int64) = safe_cast(chungl.ID_Stt as int64)
left join gioit
  on safe_cast(nmm.ID_GioiT as int64) = safe_cast(gioit.ID_Stt as int64)
left join hoatiet
  on safe_cast(nmm.ID_HoaTiet as int64) = safe_cast(hoatiet.ID_Stt as int64)
left join bost
  on safe_cast(nmm.ID_BoST as int64) = safe_cast(bost.ID_Stt as int64)
left join tendc
  on safe_cast(nmm.ID_TenDC as int64) = safe_cast(tendc.ID_Stt as int64)
left join hamlkl
  on safe_cast(nmm.ID_HamLKL as int64) = safe_cast(hamlkl.ID_Stt as int64)

left join customer_type ct
  on cast(f.ID as string) = cast(ct.ID as string)
left join receipt_purchase_type rpt
  on cast(f.ID_Phieu_thu as string) = cast(rpt.ID_Phieu_thu as string)
  and cast(f.Ma_Cong_Ty as string) = cast(rpt.Ma_Cong_Ty as string)
left join customer_purchase_type cpt
  on cast(f.ID_Phieu_thu as string) = cast(cpt.ID_Phieu_thu as string)
  and cast(f.Ma_Cong_Ty as string) = cast(cpt.Ma_Cong_Ty as string)

