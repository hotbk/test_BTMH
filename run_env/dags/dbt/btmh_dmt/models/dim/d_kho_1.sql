with src as (
    {% for s in var('dkho_sources') %}
    select
        '{{ s.code }}' as Nguon,
        Ma_Kho,
        Ten_Kho,
        Ten_KhoE,
        Thu_Kho,
        ID_NKho,
        F_LoGo,
        Dia_Chi,
        IsAmKho,
        cast(Dien_Tich as numeric) as Dien_Tich,
        Ky_Hieu,
        Ghi_Chu,
        No_TkHd,
        Co_TkHd,
        Inactive,
        current_timestamp() as UpdateTime
    from {{ source(btmh_source_name(s), 'dmkho') }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select *
from src
