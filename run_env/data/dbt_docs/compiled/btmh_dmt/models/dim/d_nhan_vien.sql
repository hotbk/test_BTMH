with src as (
    
    select
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
        Ma_Nv,
        Ten_Nv,
        Ten_NvE,
        Dia_Chi,
        Dien_Thoai,
        Noi_Lam,
        Gioi_Tinh,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmnv`
    
    
),

dedup as (
    select
        *,
        row_number() over (
            partition by Ma_Nv
            order by
                Dien_Thoai desc,
                Gioi_Tinh desc,
                Ten_Nv desc,
                Ten_NvE desc,
                Dia_Chi desc,
                Noi_Lam desc
        ) as rn
    from src
)

select
    * except(rn)
from dedup
where rn = 1