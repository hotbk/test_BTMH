with src as (
    {% for s in var('dkmp_sources') %}
    select
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
),

dedup as (
    select
        *,
        row_number() over (
            partition by Ma_KMP
            order by
                LastEdit desc,
                InsertDate desc,
                Ten_KMP desc,
                Ten_KMPE desc,
                Ma_So desc,
                Ten_Tat desc,
                ID desc
        ) as rn
    from src
)

select
    * except(rn)
from dedup
where rn = 1