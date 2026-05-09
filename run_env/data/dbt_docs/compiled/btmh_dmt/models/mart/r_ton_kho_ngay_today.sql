

select *
from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_mart`.`r_ton_kho_ngay`
-- Use the same run date as the pipeline for consistency and backfill support.
where Ngay = 
(
  
    current_date()
  
)
