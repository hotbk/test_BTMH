

with d_date as(
  SELECT 
    date,
    ma_ct_r
  FROM UNNEST(
      GENERATE_DATE_ARRAY(
          DATE '2026-01-01',
          LAST_DAY(DATE_ADD(current_date(), INTERVAL 1 QUARTER), QUARTER)
      )
  ) AS date
  CROSS JOIN UNNEST(['NM','NS']) AS ma_ct_r
)

,mua as(
  select Ngay
        ,dnx.Ma_Ct
        ,sum(So_Luong_Theo_Dvt) as so_luong
        ,sum(nx.tong_tlg * nx.so_luong_theo_dvt) as t_luong
        ,sum(T_Tien1) as gia_mua
  from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_nhap_xuat` nx left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang` h on nx.ID_Hang = h.ID
                                left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hach_toan` dnx on nx.ID_Nx = dnx.ID and nx.nguon = dnx.nguon
  where 1=1
  and nx.ID_Dv >= 0
  and dnx.Ma_Ct in ('NM', 'NS')
  and h.Ma_Nhom in ('KGB', 'KHS', 'TTMVV24KD0', 'TTNTV24KD0', 'TTVRV24KD0', 'TTXVV24KD0')
  and h.Ma_Hang <> 'NLBAC'
  and Ngay >= DATE '2026-01-01'
  and Ma_Kho not like 'KPP%' 
  group by Ngay, dnx.Ma_Ct
)

select * except(Ngay, Ma_Ct)
from d_date d left join mua m on d.date = m.Ngay and d.ma_ct_r = m.Ma_Ct