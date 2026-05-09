
    
    

with dbt_test__target as (

  select MA_MAU as unique_field
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_ma_mau_new`
  where MA_MAU is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


