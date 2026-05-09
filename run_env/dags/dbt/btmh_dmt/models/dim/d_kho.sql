with src as (
    {% for s in var('dkho_sources') %}
    select
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
),

dedup as (
    select
        *,
        row_number() over (
            partition by Ma_Kho
            order by
                F_LoGo desc,
                Dia_Chi desc,
                Ten_Kho desc,
                Ten_KhoE desc,
                Thu_Kho desc,
                ID_NKho desc,
                IsAmKho desc,
                Dien_Tich desc,
                Ky_Hieu desc,
                Ghi_Chu desc,
                No_TkHd desc,
                Co_TkHd desc,
                Inactive desc
        ) as rn
    from src
)

select
    * except(rn)
from dedup
where rn = 1
