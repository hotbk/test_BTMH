
    
    

with dbt_test__target as (

  select So_Tk as unique_field
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_ngan_hang`
  where So_Tk is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


