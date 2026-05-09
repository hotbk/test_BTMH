select distinct h.Ma_Hang, dt.Ma_Dt, dt.Ten_Dt
from `btmh-airflow-dbt-lab-2026`.`btmh_dwh_fact`.`f_nhap_xuat` nx left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hach_toan` dnx on nx.ID_Nx = dnx.ID
                                                                  and nx.nguon = dnx.nguon
                               left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_hang_1_agg` h on nx.ID_Hang = h.ID
                                                                  and nx.Nguon = h.Ma_vung
                               left join `btmh-airflow-dbt-lab-2026`.`btmh_dwh_dim`.`d_doi_tac_1` dt on nx.Ma_Dt = dt.Ma_Dt
                                                                  and nx.Nguon = dt.Nguon
where 1=1
and ID_Dv >= 0
and dnx.Ma_Ct IN ('NK','NM','NL','NS','PN')