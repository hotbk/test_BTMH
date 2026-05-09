-- Fail if d_the has more than one row per (Nguon, ID).
-- This protects downstream facts from join fan-out (duplicate fact rows).

with d as (
  select
    cast(Nguon as string) as Nguon,
    safe_cast(ID as int64) as ID
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_the`
)

select
  Nguon,
  ID,
  count(*) as cnt
from d
where ID is not null
group by 1, 2
having count(*) > 1