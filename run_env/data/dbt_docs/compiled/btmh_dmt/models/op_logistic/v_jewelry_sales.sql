with base as(
  select concat(Ma_Kho,"-",Ma_Cong_ty) as Unique_Store_ID
        ,concat(Ma_Cong_ty,"-",ID_Phieu_Thu) as Order_ID
        ,Ma_Cong_ty as Company_ID
        ,Ma_Kho as Store_ID
        ,ID_hang
        ,date(Ngay_PhieuThu) as Date
        ,h.Ma_Hang as SKU_ID
        ,Ma_Dt as Customer_ID
        ,sum(dt.So_Luong) as Sold_Qty
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_doanh_thu` dt left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_agg` h on dt.ID_Hang = h.ID
  where 1=1
  and ID_Dv >= 0
  and Ma_dong is null
  and Nganhhang_fix in ('Vàng Trang sức 24K', 'Vàng Tây')
  and Ngay_PhieuThu >= '2024-01-01'
  
  group by 1,2,3,4,5,6,7,8

   union all

  select concat(Ma_Kho,"-","NY") as Unique_Store_ID
        ,concat("B2B","-",ID_Phieu_Ban) as Order_ID
        ,'NY' as Company_ID
        ,Ma_Kho as Store_ID
        ,ID_hang
        ,date(Ngay_Chung_Tu) as Date
        ,h.Ma_Hang as SKU_ID
        ,Ma_Khach_Hang as Customer_ID
        ,sum(dt.So_Luong) as Sold_Qty
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_b2b` dt left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_agg` h on dt.ID_Hang = h.ID
  where 1=1
  and Ma_Kho = 'B2BBL'
  and Nganhhang_fix in ('Vàng Trang sức 24K', 'Vàng Tây')
  and Ngay_Chung_Tu >= '2024-01-01'
  
  group by 1,2,3,4,5,6,7,8
)

select *
from base
where 1=1