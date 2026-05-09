with src as (
    
    select
        'NY' as Nguon,
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
        'SX' as Nguon,
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
        'HD' as Nguon,
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
        'BN' as Nguon,
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
        'SG' as Nguon,
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
    
    
)

select *
from src