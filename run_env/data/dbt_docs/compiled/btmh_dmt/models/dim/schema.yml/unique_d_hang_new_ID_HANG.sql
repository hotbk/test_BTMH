
    
    

with dbt_test__target as (

  select ID_HANG as unique_field
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_new`
  where ID_HANG is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


