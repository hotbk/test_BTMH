with base as(
      select ngay
            ,nguon
            ,Ma_KhoX as Ma_Kho
            ,id_hang
            ,sum(- So_luong) as so_luong_chuyen
            ,sum(- T_Tien1) as gia_tri_chuyen
      from {{ ref('f_dieu_chuyen') }} dc
      where 1=1
      {# and id_dv >= 0 #}
      and dc.ngay >= '2024-01-01'
      group by 1,2,3,4

      union all 

      select ngay
            ,nguon
            ,Ma_KhoN as Ma_Kho
            ,id_hang
            ,sum(So_luong) as so_luong_chuyen
            ,sum(T_Tien1) as gia_tri_chuyen
      from {{ ref('f_dieu_chuyen') }} dc
      where 1=1
      {# and id_dv >= 0 #}
      and dc.ngay >= '2024-01-01'
      group by 1,2,3,4
)

select ngay
      ,nguon
      ,ma_kho
      ,id_hang
      ,sum(so_luong_chuyen) as so_luong_chuyen
      ,sum(gia_tri_chuyen) as gia_tri_chuyen
from base
group by 1,2,3,4