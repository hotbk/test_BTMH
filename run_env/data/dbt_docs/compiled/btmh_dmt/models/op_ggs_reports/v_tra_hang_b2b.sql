

with d_date as(
  SELECT 
    date,
  FROM UNNEST(
      GENERATE_DATE_ARRAY(
          DATE '2026-01-01',
          LAST_DAY(DATE_ADD(current_date(), INTERVAL 1 QUARTER), QUARTER)
      )
  ) AS date
)

,b2b as(
  select Ngay_Chung_Tu, sum(So_Luong) as so_luong, sum(SL_Chi_TT) as t_luong, sum(Doanh_Thu_Ban) as doanh_thu
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_b2b`
  where 1=1
   and Ten_Nhom_Ct = 'Vàng tích trữ'
   and Ngay_Chung_Tu >= DATE '2026-01-01'
   and Ma_kho = 'B2BBL'
  group by Ngay_Chung_Tu
)

select * except(Ngay_Chung_Tu)
from d_date d left join b2b b on d.date = b.Ngay_Chung_Tu