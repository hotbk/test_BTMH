

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
, interval 2 month)
  
)
 as start_date
)

select Ngay
      ,Nguon
      ,Ma_Kho
      ,nx.ID_Hang
      ,h.Nganhhang_fix as Nganh_hang
      ,h.Dongsp_fix as Dong_Sp
      ,h.Ten_Hang
      ,sum(So_Luong_Theo_Dvt) as quantity_by_unit
      ,sum(SL_Chi_TT) as quantity
      ,sum(T_Tien) as line_income
from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_nhap_xuat` nx left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_agg` h on nx.ID_Hang = h.ID 
where 1=1
and ID_Dv >= 0
and Ma_HT = 'NLNO'



group by 1,2,3,4,5,6,7