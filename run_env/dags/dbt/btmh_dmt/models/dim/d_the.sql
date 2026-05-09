with src as (
    {% for s in var('dthe_sources') %}
    select
        '{{ s.code }}' as Nguon,
        ID,
        ID_Dv,
        So_The,
        Ten_The as Ten_The,
        ID_Nhom,
        ID_LThe,
        ID_Dt,
        Ngay_Cap,
        Tu_Ngay,
        Den_Ngay,
        cast(Con_Lai as numeric) as Con_Lai,
        cast(So_Luong as numeric) as So_Luong,
        Tu_Seri,
        Den_Seri,
        ID_KhThe,
        DaKh,
        cast(Diem_Dk as numeric) as Diem_Dk,
        cast(Diem_DkQd as numeric) as Diem_DkQd,
        IsTheEdit,
        ID_HD,
        Ghi_Chu,
        IsAuto,
        IsNewTN,
        IsEDitTN,
        Inactive,
        InsertDate,
        LastEdit,
        UserEdit,
        UserID,
        SNgayT,
        SNgayD
    from {{ source(btmh_source_name(s), 'dmthe') }}
    where upper(Ten_The) like '%VOU%'
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select * except(_rn)
from (
    select
        src.*,
        row_number() over (
            partition by cast(src.Nguon as string), safe_cast(src.ID as int64)
            order by datetime(coalesce(src.LastEdit, src.InsertDate)) desc
        ) as _rn
    from src
)
where _rn = 1
