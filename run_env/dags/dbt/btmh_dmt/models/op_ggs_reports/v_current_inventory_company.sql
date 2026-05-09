with a as(
        select  h.Ma_Hang as Ten_Hang,
                Nguon,
                sum(tk.so_luong_ton) as ton_kho
        from {{ source('dmt', 'f_ton_kho') }} tk left join {{ ref('d_hang_1') }} h on tk.id_hang = cast(h.ID as string) and tk.nguon = h.Ma_vung 
        where 1=1
        and h.Ma_Nhom in ('KGB', 'KHS')
        and h.Ma_Hang <> 'NLBAC'
        and so_luong_ton > 0
        --and ngay = '2026-03-25'
        and ngay = current_date("Europe/Moscow")
        and ma_kho <> 'VPT'
        group by 1,2
)

select *
from a
where 1=1
and ton_kho > 0