SELECT key,
         company,
         warehouse,
         warehouse_raw,
         warehouse_name,
         open_date,
        'B2C' as channel
FROM `btmh-airflow-dbt-lab-2026`.`stg_ggs`.`d_store` 
where 1=1

union all

select 'BNOB2BBL' as key,
       'BNO' as company,
       'B2BBL' as warehouse,
       'B2BBL' as warehouse_raw,
       'Kho B2B' as warehouse_name,
        null as open_date,
       'B2B' as channel

union all

select 'BTMB2BBL' as key,
       'BTM' as company,
       'B2BBL' as warehouse,
       'B2BBL' as warehouse_raw,
       'Kho B2B' as warehouse_name,
        null as open_date,
       'B2B' as channel