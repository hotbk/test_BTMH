{{
  config(
    materialized='table',
    partition_by={"field": "date", "data_type": "date"},
    cluster_by=['Nganh_hang', 'store_code']
  )
}}

{% set selected_date = "DATE '2026-01-01'" %}

with d_date as(
  SELECT 
    date,
    nganh_hang,
    case when warehouse_raw = 'TMDT' then 'ECOM' else 'BÁN LẺ' end as channel,
		store_code.warehouse_raw as store_code
  FROM UNNEST(
      GENERATE_DATE_ARRAY(
          {{ selected_date }},
          LAST_DAY(current_date(), YEAR)
      )
  ) AS date
	CROSS JOIN UNNEST([
		'Vàng Tây',
		'Vàng Ta',
		'Tích trữ',
    'Phân loại ngoài',
		'Nguyên liệu',
		'Khác',
		'Bạc tích lũy'
	]) AS nganh_hang
	CROSS JOIN (select distinct warehouse_raw from {{ ref('d_store') }}) AS store_code
)

,hang_report as(
  select ID
        ,T_Luong
        ,case
            when nhom_sp_nho = 'BST' then 'Vàng Tây'
            when Nganh_hang = 'Vàng Tây' then 'Vàng Tây'
            when Nganh_hang = 'Khác' and nhom_sp_nho in ('Bạc', 'Phong Thủy', 'Quà tặng') then 'Khác'
            when Nganh_hang = 'Khác' and nhom_sp_nho in ('NLVT', 'NLBAC') then 'Nguyên liệu'
            when Nganh_hang = 'Khác' then 'Phân loại ngoài'
            when Nganh_hang in ('Vàng Ta', 'Tích trữ', 'Bạc tích lũy') then Nganh_hang
            else null
         end as Nganh_hang_report
  from {{ ref('d_hang_agg') }}
)

,khach_dat_raw as(
  select Ngay as Ngay
        ,case when Ma_Kho = 'TMDT' then 'ECOM' else 'BÁN LẺ' end as channel
			  ,hr.Nganh_hang_report as Nganh_hang 
				,warehouse_raw
        ,Ma_Dt
        ,Composite_ID
        ,sum(So_Luong) as so_luong_khach_dat
        ,sum(So_Luong * coalesce(hr.T_Luong,0)) as t_luong_khach_dat
        ,sum(Tong_Tien) as tien_dat
  from {{ ref('f_dat_coc') }} dc left join hang_report hr on dc.ID_Hang = hr.ID
															left join {{ ref('d_store') }} s on dc.nguon = s.company 
                                    and dc.ma_kho = s.warehouse
  where 1=1
  and ID_Dv >= 0
  and Ngay >= {{ selected_date }}
  and not (Nguon = 'HD' AND Ngay >= '2026-01-01')
  and hr.Nganh_hang_report is not null
  group by 1,2,3,4,5,6
)

,khach_dat as(
  select Ngay
        ,channel
        ,Nganh_hang
	      ,warehouse_raw
        ,count(distinct Ma_Dt) as luot_khach_dat
        ,count(distinct Composite_ID) as so_don_coc
        ,sum(so_luong_khach_dat) as so_luong_khach_dat
        ,sum(t_luong_khach_dat) as t_luong_khach_dat
        ,sum(tien_dat) as tien_dat
  from khach_dat_raw
  group by 1,2,3,4
)

,b2c_raw as(
  select Ngay_PhieuThu
      ,case when Ma_Kho = 'TMDT' then 'ECOM' else 'BÁN LẺ' end as channel
			,hr.Nganh_hang_report as Nganh_hang
		  ,warehouse_raw
        ,Ma_Dt
        ,dt.ID
        ,sum(case when (dt.Sub_ID is null or Sub_ID = '0') then So_Luong end) as so_luong_ban_ngay
        ,sum(case when (dt.Sub_ID is null or Sub_ID = '0') then SL_Chi_TT end) as t_luong_ban_ngay
        ,sum(case when (dt.Sub_ID is null or Sub_ID = '0') then Tien_PhieuThu1 end) as tien_ban_ngay
        ,sum(case when (dt.Sub_ID is not null and Sub_ID <> '0') then So_Luong end) as so_luong_tra_coc
        ,sum(case when (dt.Sub_ID is not null and Sub_ID <> '0') then SL_Chi_TT end) as t_luong_tra_coc
        ,sum(case when (dt.Sub_ID is not null and Sub_ID <> '0') then Tien_PhieuThu1 end) as tien_tra_coc
  from {{ ref('f_doanh_thu') }} dt  left join hang_report hr on dt.ID_Hang = hr.ID
																	left join {{ ref('d_store') }} s on dt.Ma_Cong_Ty = s.company 
                                    and dt.ma_kho = s.warehouse
  where 1=1
  and dt.ID_Dv >= 0
  and Ngay_PhieuThu >= {{ selected_date }}
  and not (Ma_Cong_Ty = 'HD' AND Ngay_PhieuThu >= '2026-01-01')
  and hr.Nganh_hang_report is not null
  and not (hr.Nganh_hang_report = 'Khác' and Tien_PhieuThu1 = 0)
  group by 1,2,3,4,5,6
)

,tra_coc as(
  select Ngay_PhieuThu
        ,channel
        ,Nganh_hang
		    ,warehouse_raw
        ,count(distinct Ma_Dt) as luot_khach_giao
        ,count(distinct ID) as so_hoa_don_giao
        ,sum(so_luong_ban_ngay) as so_luong_ban_ngay
        ,sum(t_luong_ban_ngay) as t_luong_ban_ngay
        ,sum(tien_ban_ngay) as tien_ban_ngay
        ,sum(so_luong_tra_coc) as so_luong_tra_coc
        ,sum(t_luong_tra_coc) as t_luong_tra_coc
        ,sum(tien_tra_coc) as tien_tra_coc
  from b2c_raw
  group by 1,2,3,4
)



select CAST(d.date AS DATE) AS date, 
       d.channel, 
	     d.store_code,
       d.Nganh_hang,
       luot_khach_giao,
       so_hoa_don_giao,
       tc.t_luong_ban_ngay, 
       kd.t_luong_khach_dat, 
       tc.t_luong_tra_coc, 
       tc.so_luong_ban_ngay, 
       kd.so_luong_khach_dat, 
       tc.so_luong_tra_coc, 
       kd.tien_dat, 
       tc.tien_ban_ngay, 
       tc.tien_tra_coc,
       t_luong_ban_ngay + t_luong_khach_dat as t_luong_ds,
       t_luong_ban_ngay + t_luong_tra_coc as t_luong_dt,
       so_luong_ban_ngay + so_luong_khach_dat as so_luong_ds,
       so_luong_ban_ngay + so_luong_tra_coc as so_luong_dt,
       tien_ban_ngay + tien_dat as doanh_so,
       tien_ban_ngay + tien_tra_coc as doanh_thu
from d_date d left join tra_coc tc on d.date = tc.Ngay_PhieuThu 
								and d.Nganh_hang = tc.Nganh_hang 
								and d.channel = tc.channel
								and d.store_code = tc.warehouse_raw
              left join khach_dat kd on d.date = kd.Ngay 
								and d.Nganh_hang = kd.Nganh_hang 
								and d.channel = kd.channel
								and d.store_code = kd.warehouse_raw
where 1=1
