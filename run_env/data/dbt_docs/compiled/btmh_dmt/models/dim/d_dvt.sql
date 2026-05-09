select
    ID,
    Ma_Dvt,
    Ten_Dvt,
    Ghi_Chu,
    current_timestamp() as UpdateTime
from `btmh-airflow-dbt-lab-2026`.`stg_augges_225`.`dmdvt`