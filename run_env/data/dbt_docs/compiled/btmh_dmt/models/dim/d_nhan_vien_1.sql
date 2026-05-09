with src as (
    
    select
        'NY' as Nguon,
        ID,
        Ma_Nv,
        Ten_Nv,
        Ten_NvE,
        Dia_Chi,
        Dien_Thoai,
        Noi_Lam,
        Gioi_Tinh,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmnv`
    
    union all
    
    
    select
        'SX' as Nguon,
        ID,
        Ma_Nv,
        Ten_Nv,
        Ten_NvE,
        Dia_Chi,
        Dien_Thoai,
        Noi_Lam,
        Gioi_Tinh,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmnv`
    
    union all
    
    
    select
        'HD' as Nguon,
        ID,
        Ma_Nv,
        Ten_Nv,
        Ten_NvE,
        Dia_Chi,
        Dien_Thoai,
        Noi_Lam,
        Gioi_Tinh,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmnv`
    
    union all
    
    
    select
        'BN' as Nguon,
        ID,
        Ma_Nv,
        Ten_Nv,
        Ten_NvE,
        Dia_Chi,
        Dien_Thoai,
        Noi_Lam,
        Gioi_Tinh,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmnv`
    
    union all
    
    
    select
        'SG' as Nguon,
        ID,
        Ma_Nv,
        Ten_Nv,
        Ten_NvE,
        Dia_Chi,
        Dien_Thoai,
        Noi_Lam,
        Gioi_Tinh,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmnv`
    
    
)

select *
from src