with src as (
    select
        *
    from {{ ref('d_doi_tac_1') }}
),

dedup as (
    select
        *,
        row_number() over (
            partition by Ma_Dt
            order by Nguon asc
        ) as rn
    from src
)

select
    * except(rn, Nguon)
from dedup
where rn = 1
