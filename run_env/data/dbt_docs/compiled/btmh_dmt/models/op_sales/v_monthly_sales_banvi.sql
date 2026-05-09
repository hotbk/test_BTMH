with b2c as(
  select Ngay_PhieuThu
        ,concat(extract(year from Ngay_PhieuThu),'/',lpad(cast(extract(month from Ngay_PhieuThu) as string),2,'0')) as year_month
        ,dt.ID as order_id
        ,Ma_Dt as customer_id
        ,case when Ma_kho = 'HD2' then 'HD1' else Ma_Kho end as Ma_Kho
        ,case when warehouse = 'TMDT' then 'ECOM' else s.channel end as channel
        ,s.warehouse_raw
        -- ,h.ID as item_id
        ,Nganhhang_fix as Nganh_hang
        ,Dongsp_fix as nhom_sp_nho
        ,ma_mau as ma_mau
        ,Ma_Hang
        ,Ten_Hang
        ,T_luong
        -- ,Ten_Hang
        ,So_Luong as quantity_by_unit
        ,SL_Chi_TT as quantity
        ,Thanh_tien_theo_DG_ban as line_amount
        ,Tien_PhieuThu1 as line_income
        ,Gia_Von as cogs
        ,Tien_PhieuThu1 - Gia_Von as gross_profit
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_doanh_thu` dt left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_agg` h on dt.ID_Hang = h.ID 
                                left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_store` s on dt.Ma_Cong_Ty = s.company 
                                    AND (
                                        CASE
                                            WHEN Ma_Kho = 'HD2' THEN 'HD1'
                                            ELSE REPLACE(Ma_Kho,'GH','CH')
                                        END
                                    ) = s.warehouse
  where 1=1
  and ID_Dv >= 0
  and Ma_Cong_Ty in ('BN', 'NY', 'SX', 'SG', 'HD')
  and not (Ma_Cong_Ty = 'HD' AND Ngay_PhieuThu >= '2026-01-01')
  and Ma_dong is null
  -- and Ma_Nhom in ('KGB', 'KHS')
  and Ngay_PhieuThu >= '2024-01-01'
  -- and Ngay_PhieuThu <= '2026-02-28'
)
,b2b as(
  select Ngay_Chung_Tu
        ,concat(extract(year from Ngay_Chung_Tu),'/',lpad(cast(extract(month from Ngay_Chung_Tu) as string),2,'0')) as year_month
        ,dt.ID_Phieu_Ban as order_id
        ,Ma_Khach_Hang as customer_id
        ,Ma_kho
        ,'B2B' as channel
        ,'B2B' as warehouse_raw
        -- ,h.ID as item_id
        ,Nganhhang_fix as Nganh_hang
        ,Dongsp_fix as nhom_sp_nho
        ,ma_mau as ma_mau
        ,h.Ma_Hang
        ,h.Ten_Hang
        ,T_Luong
        -- ,h.Ten_Hang
        ,So_Luong as quantity_by_unit
        ,SL_Chi_TT as quantity
        ,Doanh_Thu_Ban as line_amount
        ,Doanh_Thu_Ban as line_income
        ,Tong_Gia_Von as cogs
        ,Loi_Nhuan_Gop as gross_profit
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_b2b` dt left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_agg` h on dt.ID_Hang = h.ID
  where 1=1
  and Ma_Kho = 'B2BBL'
  and Ngay_Chung_Tu >= '2024-01-01'
  -- and Ma_Nhom in ('KGB', 'KHS')
)

,doanh_so_raw as(
  select concat(extract(year from created_date),'/',lpad(cast(extract(month from created_date) as string),2,'0')) as year_month
        ,warehouse_raw
        ,Nganhhang_fix as Nganh_hang
        ,Dongsp_fix as nhom_sp_nho
        ,ma_mau as ma_mau
        ,h.Ma_Hang
        ,Ten_Hang
        ,T_Luong
        ,order_id
        ,customer_id
        ,line_income
        ,quantity
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_mart`.`r_doanh_so` ds left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_store` s on ds.company_code = s.company 
                                    and warehouse_id  = s.warehouse
                                left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_agg` h on ds.item_id = cast(h.ID as string)
  where 1=1
  and created_date >= '2024-01-01'
  and warehouse_id <> 'B2BBL'
  -- and h.Ma_Nhom in ('KGB', 'KHS')
  -- group by 1,2,3,4,5
)

,doanh_so as(
  select year_month
        ,warehouse_raw
        ,Nganh_hang
        ,nhom_sp_nho
        ,ma_mau
        ,Ma_Hang
        ,Ten_Hang
        ,T_Luong
        ,count(distinct order_id) as number_of_orders_ds
        ,count(distinct customer_id) as number_of_customers_ds
        ,sum(line_income) as doanh_so 
        ,sum(quantity) as sl_theo_ds 
  from doanh_so_raw
  group by 1,2,3,4,5,6,7,8
)

,merge_data as(
  select *
  from b2c

  union all

  select *
  from b2b
)

,base as(
  select year_month,
      -- item_id, 
      Nganh_hang, 
      nhom_sp_nho,
      ma_mau,
      Ma_Hang,
      Ten_Hang,
      T_Luong,
      -- Ten_Hang, 
      warehouse_raw, 
      count(distinct order_id) as number_of_orders,
      count(distinct customer_id) as number_of_customer,
      sum(quantity_by_unit) as quantity_by_unit,
      sum(quantity) as quantity,
      sum(line_amount) as line_amount,
      sum(line_income) as line_income,
      sum(cogs) as cogs,
      sum(gross_profit) as gross_profit
from merge_data
where not (Nganh_hang = 'Khác' and line_income = 0)
group by 1,2,3,4,5,6,7,8
)

,final as(
  select b.*, ds.doanh_so, ds.sl_theo_ds, ds.number_of_orders_ds, ds.number_of_customers_ds
  from base b left join doanh_so ds on b.year_month = ds.year_month 
                  and b.warehouse_raw = ds.warehouse_raw 
                  and b.Nganh_hang = ds.Nganh_hang 
                  and b.nhom_sp_nho = ds.nhom_sp_nho
                  and b.T_Luong = ds.T_Luong
                  and b.Ma_Hang = ds.Ma_Hang
)

,final2 as(
  select  year_month
          ,CASE 
            WHEN warehouse_raw = 'TMDT' THEN 'ECOM'
            WHEN warehouse_raw = 'B2B' THEN 'B2B'
            ELSE 'RETAIL'
          END AS channel
          ,Nganh_hang
          ,nhom_sp_nho
          ,ma_mau
          ,Ma_Hang
          ,Ten_Hang
          ,T_Luong as ban_vi
          ,warehouse_raw
          ,number_of_orders
          ,number_of_customer
          ,quantity_by_unit
          ,quantity
          ,line_amount
          ,line_income
          ,cogs
          ,gross_profit
          ,case when warehouse_raw = 'B2B' then line_income else doanh_so end as doanh_so
          ,case when warehouse_raw = 'B2B' then quantity else sl_theo_ds end as sl_theo_ds
          ,case when warehouse_raw = 'B2B' then number_of_orders else number_of_orders_ds end as number_of_orders_ds
          ,case when warehouse_raw = 'B2B' then number_of_customer else number_of_customers_ds end as number_of_customers_ds
  from final 
)
select *
from final2
where 1=1
-- and warehouse_raw is null