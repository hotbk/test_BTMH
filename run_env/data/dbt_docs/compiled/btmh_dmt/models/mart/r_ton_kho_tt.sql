

with params as (
  select
    
(
  
    current_date()
  
)
 as run_date,
    
(
  
    date_sub(
(
  
    current_date()
  
)
, interval 2 month)
  
)
 as start_date
)

,a as(
        select  ngay,
                h.ID_Key as ID,
                nguon,
                ma_kho,
                case when left(h.Ma_Nhom, 3) in ('KGB', 'TTS', 'TTV', 'GC-KGB', 'GC-KHS', 'GC-ICON') then 'Thành phẩm'
                when h.Ma_Nhom in ('NLVT', 'NLTT', 'NL24') then 'Nguyên liệu'
                else 'Thành phẩm' end as Loai_Hang,
                h.Ma_Nhom,
                r.exchange_rate,
                case when (h.Ma_Nhom in ('NLVT', 'NLTT', 'NL24') ) then sum(tk.so_luong_ton * r.exchange_rate) else sum(tk.so_luong_ton * T_Luong) end as ton_kho,
                sum(gia_tri_ton) as gia_tri_ton
        from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_ton_kho` tk left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_1` h on tk.id_hang = cast(h.ID as string) and tk.nguon = h.Ma_vung 
                                        left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_nhom` n on h.ID_Nhom = n.ID
                                        left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_mart`.`r_exchange_rate` r on h.Ten_Hang = r.Ten_Hang
        where 1=1
        and h.Ma_Nhom in ('KGB', 'KHS', 'NLTT', 'TTMVV24KD0', 'TTNTV24KD0', 'TTVRV24KD0', 'TTXVV24KD0', 'NLVT', 'NL24', 'GC-KGB', 'GC-KHS', 'GC-ICON')
        and h.Ma_Hang <> 'NLBAC'
        and so_luong_ton > 0
        and h.Ten_Hang <> 'Mẫu màu%'
        and h.Ten_Hang <> '%vẩy%'
        and h.Ten_Hang <> '%vảy%'
        -- and ngay = '2026-03-09'
        and ma_kho <> 'VPT'
        group by 1,2,3,4,5,6,7
)

select *
from a
where 1=1
and ton_kho > 0

