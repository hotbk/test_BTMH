select *
from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_mart`.`v_stock_history`
where Ngay = 
(
  
    current_date()
  
)
