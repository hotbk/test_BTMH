with src as (
    
    select
        'NY' as Nguon,
        ID,
        ID_Dv,
        Ma_KMP,
        Ma_So,
        Ten_KMP,
        Ten_KMPE,
        Ten_Tat,
        D_v,
        ID_Nhom,
        ID_Loai,
        No_Tk,
        Co_Tk,
        Tk_Dt,
        Tk_Ck,
        Tk_Tl,
        cast(Thue as numeric) as Thue,
        ListMaCt,
        Ghi_Chu,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmkmp`
    
    union all
    
    
    select
        'SX' as Nguon,
        ID,
        ID_Dv,
        Ma_KMP,
        Ma_So,
        Ten_KMP,
        Ten_KMPE,
        Ten_Tat,
        D_v,
        ID_Nhom,
        ID_Loai,
        No_Tk,
        Co_Tk,
        Tk_Dt,
        Tk_Ck,
        Tk_Tl,
        cast(Thue as numeric) as Thue,
        ListMaCt,
        Ghi_Chu,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_224`.`dmkmp`
    
    union all
    
    
    select
        'HD' as Nguon,
        ID,
        ID_Dv,
        Ma_KMP,
        Ma_So,
        Ten_KMP,
        Ten_KMPE,
        Ten_Tat,
        D_v,
        ID_Nhom,
        ID_Loai,
        No_Tk,
        Co_Tk,
        Tk_Dt,
        Tk_Ck,
        Tk_Tl,
        cast(Thue as numeric) as Thue,
        ListMaCt,
        Ghi_Chu,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_226`.`dmkmp`
    
    union all
    
    
    select
        'BN' as Nguon,
        ID,
        ID_Dv,
        Ma_KMP,
        Ma_So,
        Ten_KMP,
        Ten_KMPE,
        Ten_Tat,
        D_v,
        ID_Nhom,
        ID_Loai,
        No_Tk,
        Co_Tk,
        Tk_Dt,
        Tk_Ck,
        Tk_Tl,
        cast(Thue as numeric) as Thue,
        ListMaCt,
        Ghi_Chu,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_227`.`dmkmp`
    
    union all
    
    
    select
        'SG' as Nguon,
        ID,
        ID_Dv,
        Ma_KMP,
        Ma_So,
        Ten_KMP,
        Ten_KMPE,
        Ten_Tat,
        D_v,
        ID_Nhom,
        ID_Loai,
        No_Tk,
        Co_Tk,
        Tk_Dt,
        Tk_Ck,
        Tk_Tl,
        cast(Thue as numeric) as Thue,
        ListMaCt,
        Ghi_Chu,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        current_timestamp() as UpdateTime
    from `btmh-airflow-dbt-lab-2026`.`stg_augges_sg`.`dmkmp`
    
    
)

select *
from src