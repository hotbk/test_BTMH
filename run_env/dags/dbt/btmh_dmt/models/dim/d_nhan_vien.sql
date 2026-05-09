with src as (
    {% for s in var('dnhanvien_sources') %}
    select
        Ma_Nv,
        Ten_Nv,
        Ten_NvE,
        Dia_Chi,
        Dien_Thoai,
        Noi_Lam,
        Gioi_Tinh,
        current_timestamp() as UpdateTime
    from {{ source(btmh_source_name(s), 'dmnv') }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
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
