with src as (
    
    select
        Ma_Kho,
        Ten_Kho,
        Ten_KhoE,
        Thu_Kho,
        ID_NKho,
        F_LoGo,
        Dia_Chi,
        IsAmKho,
        cast(Dien_Tich as numeric) as Dien_Tich,
        Ky_Hieu,
        Ghi_Chu,
        No_TkHd,
        Co_TkHd,
        Inactive,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmkho`
    
    union all
    
    
    select
        Ma_Kho,
        Ten_Kho,
        Ten_KhoE,
        Thu_Kho,
        ID_NKho,
        F_LoGo,
        Dia_Chi,
        IsAmKho,
        cast(Dien_Tich as numeric) as Dien_Tich,
        Ky_Hieu,
        Ghi_Chu,
        No_TkHd,
        Co_TkHd,
        Inactive,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmkho`
    
    union all
    
    
    select
        Ma_Kho,
        Ten_Kho,
        Ten_KhoE,
        Thu_Kho,
        ID_NKho,
        F_LoGo,
        Dia_Chi,
        IsAmKho,
        cast(Dien_Tich as numeric) as Dien_Tich,
        Ky_Hieu,
        Ghi_Chu,
        No_TkHd,
        Co_TkHd,
        Inactive,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmkho`
    
    union all
    
    
    select
        Ma_Kho,
        Ten_Kho,
        Ten_KhoE,
        Thu_Kho,
        ID_NKho,
        F_LoGo,
        Dia_Chi,
        IsAmKho,
        cast(Dien_Tich as numeric) as Dien_Tich,
        Ky_Hieu,
        Ghi_Chu,
        No_TkHd,
        Co_TkHd,
        Inactive,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmkho`
    
    union all
    
    
    select
        Ma_Kho,
        Ten_Kho,
        Ten_KhoE,
        Thu_Kho,
        ID_NKho,
        F_LoGo,
        Dia_Chi,
        IsAmKho,
        cast(Dien_Tich as numeric) as Dien_Tich,
        Ky_Hieu,
        Ghi_Chu,
        No_TkHd,
        Co_TkHd,
        Inactive,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmkho`
    
    
),

dedup as (
    select
        *,
        row_number() over (
            partition by Ma_Kho
            order by
                F_LoGo desc,
                Dia_Chi desc,
                Ten_Kho desc,
                Ten_KhoE desc,
                Thu_Kho desc,
                ID_NKho desc,
                IsAmKho desc,
                Dien_Tich desc,
                Ky_Hieu desc,
                Ghi_Chu desc,
                No_TkHd desc,
                Co_TkHd desc,
                Inactive desc
        ) as rn
    from src
)

select
    * except(rn)
from dedup
where rn = 1