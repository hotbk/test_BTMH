select
    ID,
    Ma_Nganh,
    Ten_Nganh,
    Ten_NganhE,
    current_timestamp() as UpdateTime
from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmnganh`