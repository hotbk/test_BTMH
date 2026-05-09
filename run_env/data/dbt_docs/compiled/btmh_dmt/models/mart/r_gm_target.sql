SELECT
    d AS date,
    gm_target,
    gm_floor
FROM `btmh-airflow-dbt-lab-2026`.`stg_ggs`.`ggs_gm_target`,
UNNEST(
    GENERATE_DATE_ARRAY(start_date, end_date)
) AS d

ORDER BY date