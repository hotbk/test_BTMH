
    
    

with dbt_test__target as (

  select Composite_ID as unique_field
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_dat_coc`
  where Composite_ID is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


