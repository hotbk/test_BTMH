

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

select Ngay
      ,h.ID_Key as ID
      ,dnx.Ma_Ct
      ,sum(
        case 
            when h.ma_nhom in ('NLVT','NLTT','NL24')
            then nx.so_luong_theo_dvt
            else nx.tong_tlg * nx.so_luong_theo_dvt
        end
      ) as quantity
from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_nhap_xuat` nx left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_1` h on nx.ID_Hang = h.ID and nx.Nguon = h.Ma_vung
                                 left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hach_toan` dnx on nx.ID_Nx = dnx.ID and nx.nguon = dnx.nguon
where 1=1
and nx.ID_Dv >= 0
and dnx.Ma_Ct in ('NM', 'NS')
and Ma_Kho not like 'KPP%'
and h.Ma_Nhom in ('TT-24KD0', 'KGB', 'KHS', 'KTD', 'NLTT', 'TTMVV24KD0', 'TTNTV24KD0', 'TTVRV24KD0', 'TTXVV24KD0', 'NLVT', 'NL24')
and h.Ma_Hang <> 'NLBAC'




group by Ngay, h.ID_Key, h.Ma_Nhom, dnx.Ma_Ct