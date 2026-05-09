select
    ID,
    Ma_Nhom,
    Ten_Nhom,
    Ten_NhomE,
    ID_NhomMe,
    Cap_Nhom,
    SubID,
    SoTT,
    current_timestamp() as UpdateTime
from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmnh`