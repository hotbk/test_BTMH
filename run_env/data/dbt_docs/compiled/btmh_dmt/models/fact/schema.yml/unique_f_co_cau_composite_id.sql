
    
    

with dbt_test__target as (

  select composite_id as unique_field
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_co_cau`
  where composite_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


