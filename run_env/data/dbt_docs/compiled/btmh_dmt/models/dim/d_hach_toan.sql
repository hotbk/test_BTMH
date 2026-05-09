with src as (
    
    select
        ID,
        'NY' as Nguon,
        Ma_Ct,
        Ma_Nx as Ma_HT,
        Ten_Nx as Ten_HT,
        No_tk,
        Co_Tk,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmnx`
    
    union all
    
    
    select
        ID,
        'SX' as Nguon,
        Ma_Ct,
        Ma_Nx as Ma_HT,
        Ten_Nx as Ten_HT,
        No_tk,
        Co_Tk,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmnx`
    
    union all
    
    
    select
        ID,
        'HD' as Nguon,
        Ma_Ct,
        Ma_Nx as Ma_HT,
        Ten_Nx as Ten_HT,
        No_tk,
        Co_Tk,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmnx`
    
    union all
    
    
    select
        ID,
        'BN' as Nguon,
        Ma_Ct,
        Ma_Nx as Ma_HT,
        Ten_Nx as Ten_HT,
        No_tk,
        Co_Tk,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmnx`
    
    union all
    
    
    select
        ID,
        'SG' as Nguon,
        Ma_Ct,
        Ma_Nx as Ma_HT,
        Ten_Nx as Ten_HT,
        No_tk,
        Co_Tk,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmnx`
    
    
)

select distinct * from src