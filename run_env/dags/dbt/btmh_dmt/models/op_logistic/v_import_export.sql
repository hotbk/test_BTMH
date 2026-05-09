select ngay
      ,nx.nguon
      ,ma_kho
      ,id_hang
      ,sum( case when dnx.Ma_Ct IN ('NK','NM','NL','NS','PN') then So_Luong_Theo_Dvt end) as so_luong_nhap
      ,sum( case when dnx.Ma_Ct not IN ('NK','NM','NL','NS','PN') then So_Luong_Theo_Dvt end) as so_luong_xuat
      ,sum( case when dnx.Ma_Ct IN ('NK','NM','NL','NS','PN') then T_Tien1 end) as gia_tri_nhap
      ,sum( case when dnx.Ma_Ct not IN ('NK','NM','NL','NS','PN') then T_Tien1 end) as gia_tri_xuat
from {{ ref('f_nhap_xuat') }} nx left join {{ ref('d_hach_toan') }} dnx on nx.ID_Nx = dnx.ID and nx.nguon = dnx.nguon
where 1=1
and id_dv >= 0
and nx.ngay >= '2024-01-01'
group by 1,2,3,4