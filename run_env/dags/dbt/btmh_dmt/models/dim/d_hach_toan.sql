with src as (
    {% for s in var('dmnx_sources') %}
    select
        ID,
        '{{ s.code }}' as Nguon,
        Ma_Ct,
        Ma_Nx as Ma_HT,
        Ten_Nx as Ten_HT,
        No_tk,
        Co_Tk,
        current_timestamp() as UpdateTime
    from {{ source(btmh_source_name(s), 'dmnx') }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select distinct * from src
