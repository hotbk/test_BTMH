with src as (
    {% for s in var('dnhanvien_sources') %}
    select
        '{{ s.code }}' as Nguon,
        ID,
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
)

select *
from src
