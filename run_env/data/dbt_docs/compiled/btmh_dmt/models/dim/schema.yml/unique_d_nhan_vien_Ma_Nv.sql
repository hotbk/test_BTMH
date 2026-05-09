
    
    

with dbt_test__target as (

  select Ma_Nv as unique_field
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhan_vien`
  where Ma_Nv is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


