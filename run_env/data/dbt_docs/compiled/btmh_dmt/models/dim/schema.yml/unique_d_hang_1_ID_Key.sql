
    
    

with dbt_test__target as (

  select ID_Key as unique_field
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_1`
  where ID_Key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


