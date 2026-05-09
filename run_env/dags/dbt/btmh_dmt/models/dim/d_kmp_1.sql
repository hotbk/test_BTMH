with src as (
    {% for s in var('dkmp_sources') %}
    select
        '{{ s.code }}' as Nguon,
        ID,
        ID_Dv,
        Ma_KMP,
        Ma_So,
        Ten_KMP,
        Ten_KMPE,
        Ten_Tat,
        D_v,
        ID_Nhom,
        ID_Loai,
        No_Tk,
        Co_Tk,
        Tk_Dt,
        Tk_Ck,
        Tk_Tl,
        cast(Thue as numeric) as Thue,
        ListMaCt,
        Ghi_Chu,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        current_timestamp() as UpdateTime
    from {{ source(btmh_source_name(s), 'dmkmp') }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select *
from src