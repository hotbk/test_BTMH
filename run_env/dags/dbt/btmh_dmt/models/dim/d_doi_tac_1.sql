with src as (
    {% for s in var('ddoitac_sources') %}
    select
        '{{ s.code }}' as Nguon,
        dt.ID,
        dt.Ma_Dt,
        dt.Ten_Dt as Ten_Dt,
        dt.Dia_chi as Dia_chi,
        dt.Dien_Thoai,
        dt.Gioi_Tinh as Gioi_Tinh,
        dt.Ngay_Sinh,
        dt.So_CMT,
        t.Ten_tinh as Tinh,
        q.Ten_Quan as Quan,
        p.Ten_Phuong as Phuong,
        current_timestamp() as UpdateTime
    from {{ source(btmh_source_name(s), 'dmdt') }} dt
    left join {{ source(btmh_source_name(s), 'dmtinh') }} t
        on dt.ID_Tinh = t.ID
    left join {{ source(btmh_source_name(s), 'dmquan') }} q
        on dt.ID_Quan = q.ID
    left join {{ source(btmh_source_name(s), 'dmphuong') }} p
        on dt.ID_Phuong = p.ID
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select * from src
